//! CDC via WAL logical decoding (slot + `test_decoding`), sem triggers.
//!
//! Usa `pg_logical_slot_peek_changes` (lê sem consumir) → publica → e só então
//! `pg_logical_slot_get_changes` avança o slot. Mantém at-least-once (em crash o
//! slot rebobina) → Inbox continua deduplicando. Ver `SPEC.md` §13.1.

use tokio_postgres::Client;
use uhura_core::{Error, Result};

const PLUGIN: &str = "test_decoding";

/// Operação de uma mudança de linha.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    /// Nome do evento Uhura correspondente.
    pub fn event(self) -> &'static str {
        match self {
            ChangeOp::Insert => "inserted",
            ChangeOp::Update => "updated",
            ChangeOp::Delete => "removed",
        }
    }
}

/// Uma mudança decodificada do WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub schema: String,
    pub table: String,
    pub op: ChangeOp,
    pub columns: Vec<(String, String)>,
}

impl Change {
    /// Valor de uma coluna.
    pub fn column(&self, name: &str) -> Option<&str> {
        self.columns
            .iter()
            .find(|(c, _)| c == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Cria o replication slot (logical, `test_decoding`) se ainda não existir.
pub async fn ensure_slot(client: &Client, slot: &str) -> Result<()> {
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| Error::Storage(format!("checar slot: {e}")))?;
    if exists.is_some() {
        return Ok(());
    }
    client
        .query(
            "SELECT pg_create_logical_replication_slot($1, $2)",
            &[&slot, &PLUGIN],
        )
        .await
        .map_err(|e| Error::Storage(format!("criar slot (wal_level=logical?): {e}")))?;
    tracing::info!(slot, "replication slot criado");
    Ok(())
}

/// Lê (sem consumir) até `max` mudanças do slot: pares `(lsn, linha decodificada)`.
pub async fn peek_changes(client: &Client, slot: &str, max: i32) -> Result<Vec<(String, String)>> {
    let rows = client
        .query(
            "SELECT lsn::text AS lsn, data FROM pg_logical_slot_peek_changes($1, NULL, $2)",
            &[&slot, &max],
        )
        .await
        .map_err(|e| Error::Storage(format!("peek changes: {e}")))?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, String>("lsn"), r.get::<_, String>("data")))
        .collect())
}

/// Consome o slot até `upto_lsn` (avança o cursor durável).
pub async fn advance(client: &Client, slot: &str, upto_lsn: &str) -> Result<()> {
    // Valida o LSN (vem do próprio Postgres) e o inlina — `pg_lsn` não aceita
    // bind de String como parâmetro.
    if upto_lsn.is_empty() || !upto_lsn.chars().all(|c| c.is_ascii_hexdigit() || c == '/') {
        return Err(Error::Storage(format!("LSN inválido: {upto_lsn}")));
    }
    let sql = format!("SELECT 1 FROM pg_logical_slot_get_changes($1, '{upto_lsn}'::pg_lsn, NULL)");
    client
        .query(sql.as_str(), &[&slot])
        .await
        .map_err(|e| Error::Storage(format!("advance slot: {e}")))?;
    Ok(())
}

/// Decodifica uma linha do `test_decoding`. `None` para BEGIN/COMMIT/etc.
pub fn parse_test_decoding(line: &str) -> Option<Change> {
    let rest = line.strip_prefix("table ")?;
    // "schema.tabela: OP: colunas"
    let (table_part, after) = rest.split_once(": ")?;
    let (op_str, cols_str) = after.split_once(": ").unwrap_or((after, ""));
    let (schema, table) = table_part.split_once('.')?;
    let op = match op_str {
        "INSERT" => ChangeOp::Insert,
        "UPDATE" => ChangeOp::Update,
        "DELETE" => ChangeOp::Delete,
        _ => return None,
    };
    // UPDATE com REPLICA IDENTITY FULL: `old-key: <old> new-tuple: <new>` → novo.
    let cols_str = match cols_str.find("new-tuple:") {
        Some(idx) => &cols_str[idx + "new-tuple:".len()..],
        None => cols_str,
    };
    Some(Change {
        schema: schema.trim().to_string(),
        table: table.trim().to_string(),
        op,
        columns: parse_columns(cols_str.trim()),
    })
}

/// Parseia `nome[tipo]:valor nome[tipo]:valor ...`, respeitando valores entre aspas.
fn parse_columns(s: &str) -> Vec<(String, String)> {
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    let n = chars.len();
    let mut out = Vec::new();

    while i < n {
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }
        // nome até '['
        let name_start = i;
        while i < n && chars[i] != '[' {
            i += 1;
        }
        if i >= n {
            break;
        }
        let name: String = chars[name_start..i].iter().collect();
        // pula [tipo]
        while i < n && chars[i] != ']' {
            i += 1;
        }
        if i < n {
            i += 1; // ']'
        }
        // pula ':'
        if i < n && chars[i] == ':' {
            i += 1;
        }
        // valor: aspas ou até espaço
        let value = if i < n && chars[i] == '\'' {
            i += 1;
            let mut v = String::new();
            while i < n {
                if chars[i] == '\'' {
                    // '' = aspa escapada
                    if i + 1 < n && chars[i + 1] == '\'' {
                        v.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    v.push(chars[i]);
                    i += 1;
                }
            }
            v
        } else {
            let start = i;
            while i < n && !chars[i].is_whitespace() {
                i += 1;
            }
            chars[start..i].iter().collect()
        };
        out.push((name.trim().to_string(), value));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_insert() {
        let line =
            "table public.cliente: INSERT: id[text]:'c1' nome[text]:'Ana Paula' saldo[integer]:100";
        let c = parse_test_decoding(line).unwrap();
        assert_eq!(c.schema, "public");
        assert_eq!(c.table, "cliente");
        assert_eq!(c.op, ChangeOp::Insert);
        assert_eq!(c.column("id"), Some("c1"));
        assert_eq!(c.column("nome"), Some("Ana Paula"));
        assert_eq!(c.column("saldo"), Some("100"));
    }

    #[test]
    fn parses_delete_and_skips_begin() {
        assert!(parse_test_decoding("BEGIN 700").is_none());
        assert!(parse_test_decoding("COMMIT 700").is_none());
        let c = parse_test_decoding("table public.conta: DELETE: id[text]:'a1'").unwrap();
        assert_eq!(c.op, ChangeOp::Delete);
        assert_eq!(c.column("id"), Some("a1"));
    }

    #[test]
    fn update_full_uses_new_tuple() {
        let line = "table public.diag: UPDATE: old-key: id[text]:'x1' nome[text]:'Ana' new-tuple: id[text]:'x1' nome[text]:'Bia'";
        let c = parse_test_decoding(line).unwrap();
        assert_eq!(c.op, ChangeOp::Update);
        assert_eq!(c.column("id"), Some("x1"));
        assert_eq!(c.column("nome"), Some("Bia"));
    }

    #[test]
    fn handles_escaped_quote() {
        let c = parse_test_decoding("table s.t: INSERT: nome[text]:'O''Brien'").unwrap();
        assert_eq!(c.column("nome"), Some("O'Brien"));
    }
}
