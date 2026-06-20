//! Operações de schema (`uhura db init` / `uhura db sync`).

use std::fs;

use serde::Deserialize;
use uhura_core::{Error, Result};

/// DDL idempotente do schema base (outbox/inbox + trigger de NOTIFY).
const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
/// Função genérica de captura CDC.
const CDC_FUNCTION_SQL: &str = include_str!("../sql/cdc.sql");

/// Entrada de um arquivo `.cdc` (JSON5: aceita comentários).
#[derive(Debug, Deserialize)]
pub struct CdcSpec {
    /// Database de origem (informativo na v1 — single-database).
    #[serde(default)]
    pub database: Option<String>,
    /// Tabela de negócio observada.
    pub table: String,
    /// Eventos a capturar: `inserted` | `updated` | `removed`.
    pub events: Vec<String>,
    /// Coluna usada como id/partição.
    #[serde(rename = "id_column")]
    pub id_column: String,
    /// Domínio do contrato gerado (ex.: `usuario.info`).
    pub contract: String,
}

/// Cria/atualiza as tabelas `uhura_outbox` e `uhura_inbox` e o trigger de notify.
pub async fn init(postgres_url: &str) -> Result<()> {
    let client = crate::connect(postgres_url).await?;
    client
        .batch_execute(SCHEMA_SQL)
        .await
        .map_err(|e| Error::Storage(format!("db init: {e}")))?;
    tracing::info!("schema Uhura aplicado (uhura_outbox, uhura_inbox, trigger notify)");
    Ok(())
}

/// Gera/aplica triggers de CDC a partir dos arquivos `.cdc` em `cdc_dir`.
pub async fn sync(postgres_url: &str, cdc_dir: &str) -> Result<()> {
    let specs = read_specs(cdc_dir)?;
    if specs.is_empty() {
        tracing::warn!(dir = %cdc_dir, "nenhum arquivo .cdc encontrado");
        return Ok(());
    }

    let client = crate::connect(postgres_url).await?;
    client
        .batch_execute(CDC_FUNCTION_SQL)
        .await
        .map_err(|e| Error::Storage(format!("função CDC: {e}")))?;

    for spec in &specs {
        let sql = trigger_sql(spec)?;
        client
            .batch_execute(&sql)
            .await
            .map_err(|e| Error::Storage(format!("trigger {}: {e}", spec.table)))?;
        tracing::info!(
            table = %spec.table,
            contract = %spec.contract,
            events = ?spec.events,
            "trigger CDC aplicado"
        );
    }
    Ok(())
}

/// Lê e parseia todos os arquivos `.cdc` (JSON5) de um diretório.
pub fn read_specs(dir: &str) -> Result<Vec<CdcSpec>> {
    let entries =
        fs::read_dir(dir).map_err(|e| Error::Storage(format!("ler diretório {dir}: {e}")))?;
    let mut out = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| Error::Storage(format!("entrada do diretório: {e}")))?
            .path();
        if path.extension().and_then(|s| s.to_str()) == Some("cdc") {
            let content = fs::read_to_string(&path)
                .map_err(|e| Error::Storage(format!("ler {path:?}: {e}")))?;
            let specs: Vec<CdcSpec> = json5::from_str(&content)
                .map_err(|e| Error::Storage(format!("parse {path:?}: {e}")))?;
            out.extend(specs);
        }
    }
    Ok(out)
}

fn trigger_sql(spec: &CdcSpec) -> Result<String> {
    let ops = ops_clause(&spec.events)?;
    let trigger = format!(
        "uhura_cdc_{}",
        spec.table.replace(|c: char| !c.is_alphanumeric(), "_")
    );
    let contract = spec.contract.replace('\'', "''");
    let id_col = spec.id_column.replace('\'', "''");
    let table = &spec.table;
    Ok(format!(
        "DROP TRIGGER IF EXISTS {trigger} ON \"{table}\";\n\
         CREATE TRIGGER {trigger} AFTER {ops} ON \"{table}\" \
         FOR EACH ROW EXECUTE FUNCTION uhura_cdc_capture('{contract}', '{id_col}');"
    ))
}

fn ops_clause(events: &[String]) -> Result<String> {
    let mut ops = Vec::new();
    for event in events {
        match event.as_str() {
            "inserted" => ops.push("INSERT"),
            "updated" => ops.push("UPDATE"),
            "removed" => ops.push("DELETE"),
            other => return Err(Error::Storage(format!("evento CDC inválido: {other}"))),
        }
    }
    if ops.is_empty() {
        return Err(Error::Storage("lista de eventos CDC vazia".to_string()));
    }
    Ok(ops.join(" OR "))
}
