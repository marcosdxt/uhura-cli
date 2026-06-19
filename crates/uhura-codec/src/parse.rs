//! Parser dos contratos TypeScript (`@UhuraContract`) via tree-sitter.

use serde::Deserialize;
use tree_sitter::{Parser, Query, QueryCursor};
use uhura_core::{Error, Result};

/// Um contrato extraído de um arquivo `.ts`.
#[derive(Debug, Clone)]
pub struct Contract {
    pub name: String,
    pub domain: String,
    pub events: Vec<String>,
    pub partition_id: Option<String>,
    pub fields: Vec<Field>,
}

/// Um campo de um contrato.
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub ts_type: String,
    pub optional: bool,
}

#[derive(Debug, Deserialize)]
struct ContractMeta {
    domain: String,
    #[serde(default)]
    events: Vec<String>,
    #[serde(rename = "partitionId", default)]
    partition_id: Option<String>,
}

const QUERY: &str = r#"
(export_statement
  decorator: (decorator
    (call_expression
      function: (identifier) @deco
      arguments: (arguments (object) @args)))
  declaration: (class_declaration
    name: (type_identifier) @name
    body: (class_body) @body))

(class_declaration
  decorator: (decorator
    (call_expression
      function: (identifier) @deco
      arguments: (arguments (object) @args)))
  name: (type_identifier) @name
  body: (class_body) @body)
"#;

/// Extrai os contratos de um fonte TypeScript.
pub fn parse_source(src: &str) -> Result<Vec<Contract>> {
    let language = tree_sitter_typescript::language_typescript();
    let mut parser = Parser::new();
    parser
        .set_language(&language)
        .map_err(|e| Error::Codec(format!("tree-sitter: {e}")))?;
    let tree = parser
        .parse(src, None)
        .ok_or_else(|| Error::Codec("falha ao parsear TS".to_string()))?;

    let query =
        Query::new(&language, QUERY).map_err(|e| Error::Codec(format!("query inválida: {e}")))?;
    let names = query.capture_names();
    let bytes = src.as_bytes();

    let mut cursor = QueryCursor::new();
    let mut contracts = Vec::new();
    for m in cursor.matches(&query, tree.root_node(), bytes) {
        let mut deco = "";
        let mut args = "";
        let mut name = "";
        let mut body = None;
        for cap in m.captures {
            let text = cap.node.utf8_text(bytes).unwrap_or("");
            match names[cap.index as usize] {
                "deco" => deco = text,
                "args" => args = text,
                "name" => name = text,
                "body" => body = Some(cap.node),
                _ => {}
            }
        }
        if deco != "UhuraContract" {
            continue;
        }
        let meta: ContractMeta =
            json5::from_str(args).map_err(|e| Error::Codec(format!("decorator de {name}: {e}")))?;

        let mut fields = Vec::new();
        if let Some(body) = body {
            let mut walk = body.walk();
            for child in body.children(&mut walk) {
                if matches!(
                    child.kind(),
                    "public_field_definition" | "property_signature"
                ) {
                    if let Some(field) = parse_field(child.utf8_text(bytes).unwrap_or("")) {
                        fields.push(field);
                    }
                }
            }
        }

        contracts.push(Contract {
            name: name.to_string(),
            domain: meta.domain,
            events: meta.events,
            partition_id: meta.partition_id,
            fields,
        });
    }
    Ok(contracts)
}

fn parse_field(text: &str) -> Option<Field> {
    let t = text.trim().trim_end_matches(';').trim();
    let colon = t.find(':')?;
    let name_part = t[..colon].trim();
    let type_part = t[colon + 1..].split('=').next()?.trim();
    if type_part.is_empty() {
        return None;
    }
    let optional = name_part.ends_with('?');
    let name = name_part.trim_end_matches('?').trim();
    // Descarta modificadores (readonly/public/...) ficando com o último token.
    let name = name.split_whitespace().next_back()?;
    if name.is_empty() {
        return None;
    }
    Some(Field {
        name: name.to_string(),
        ts_type: type_part.to_string(),
        optional,
    })
}

/// Mapeia um tipo TypeScript para o equivalente Rust.
pub fn ts_to_rust(ts: &str) -> String {
    let ts = ts.trim();
    if let Some(inner) = ts.strip_suffix("[]") {
        return format!("Vec<{}>", ts_to_rust(inner));
    }
    if let Some(rest) = ts.strip_prefix("Array<") {
        if let Some(inner) = rest.strip_suffix('>') {
            return format!("Vec<{}>", ts_to_rust(inner));
        }
    }
    match ts {
        "string" => "String".to_string(),
        "number" => "f64".to_string(),
        "boolean" => "bool".to_string(),
        "Date" => "chrono::DateTime<chrono::Utc>".to_string(),
        "any" | "unknown" | "object" => "serde_json::Value".to_string(),
        other => {
            // Tipo customizado (outro contrato) começa com maiúscula; senão, fallback.
            if other.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                other.to_string()
            } else {
                "serde_json::Value".to_string()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exported_contract() {
        let src = "@UhuraContract({ domain: 'usuario.info', events: ['started','stopped'], \
                   partitionId: 'id' })\nexport class UsuarioInfo {\n  id: string;\n  \
                   active?: boolean;\n  tags: string[];\n}\n";
        let contracts = parse_source(src).unwrap();
        assert_eq!(contracts.len(), 1);
        let c = &contracts[0];
        assert_eq!(c.name, "UsuarioInfo");
        assert_eq!(c.domain, "usuario.info");
        assert_eq!(c.events, vec!["started", "stopped"]);
        assert_eq!(c.partition_id.as_deref(), Some("id"));
        assert_eq!(c.fields.len(), 3);
        let active = c.fields.iter().find(|f| f.name == "active").unwrap();
        assert!(active.optional);
    }

    #[test]
    fn maps_types() {
        assert_eq!(ts_to_rust("string"), "String");
        assert_eq!(ts_to_rust("number"), "f64");
        assert_eq!(ts_to_rust("boolean"), "bool");
        assert_eq!(ts_to_rust("Date"), "chrono::DateTime<chrono::Utc>");
        assert_eq!(ts_to_rust("string[]"), "Vec<String>");
        assert_eq!(ts_to_rust("Array<number>"), "Vec<f64>");
    }
}
