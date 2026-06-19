//! Envelope CloudEvents 1.0 + extensões Uhura (ver `SPEC.md` §7).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Versão da spec CloudEvents suportada.
pub const CLOUDEVENTS_SPEC_VERSION: &str = "1.0";

/// Natureza do fato carregado pelo envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum FactType {
    /// Evento de domínio (`uhura.publish`).
    Event,
    /// Estado completo de uma entidade (CDC).
    Snapshot,
    /// Mudança incremental de uma entidade (CDC).
    Delta,
}

/// Envelope padrão de toda mensagem do bus.
///
/// Os nomes serializados seguem CloudEvents 1.0; as extensões Uhura/W3C
/// (`partitionkey`, `sequence`, `facttype`, `traceparent`, `tracestate`)
/// são atributos de topo, conforme o binding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Identificador único; chave de deduplicação no Inbox.
    pub id: String,
    /// Serviço/instância produtor.
    pub source: String,
    /// Versão da spec CloudEvents.
    #[serde(rename = "specversion")]
    pub spec_version: String,
    /// `<domínio>.<evento>`.
    #[serde(rename = "type")]
    pub r#type: String,

    /// Id da partição/entidade (= `partitionkey`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Timestamp do fato.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<DateTime<Utc>>,
    /// Tipo de conteúdo de `data` (default `application/json`).
    #[serde(rename = "datacontenttype", skip_serializing_if = "Option::is_none")]
    pub data_content_type: Option<String>,
    /// URI/versão do schema do contrato.
    #[serde(rename = "dataschema", skip_serializing_if = "Option::is_none")]
    pub data_schema: Option<String>,

    // --- extensões Uhura / W3C ---
    /// Chave de ordenação (roteamento consistent-hash).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitionkey: Option<String>,
    /// Sequência monotônica por partição (guarda de ordem).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Natureza do fato.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facttype: Option<FactType>,
    /// Contexto de trace W3C.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// Estado de trace W3C.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,

    /// Conteúdo do contrato.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl Envelope {
    /// Cria um envelope mínimo válido (CloudEvents 1.0).
    pub fn new(id: impl Into<String>, source: impl Into<String>, ty: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            source: source.into(),
            spec_version: CLOUDEVENTS_SPEC_VERSION.to_string(),
            r#type: ty.into(),
            subject: None,
            time: None,
            data_content_type: Some("application/json".to_string()),
            data_schema: None,
            partitionkey: None,
            sequence: None,
            facttype: None,
            traceparent: None,
            tracestate: None,
            data: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_with_cloudevents_field_names() {
        let mut e = Envelope::new("1", "svc", "uhura.acme.usuario.info.started");
        e.partitionkey = Some("42".to_string());
        e.facttype = Some(FactType::Event);

        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["specversion"], "1.0");
        assert_eq!(v["type"], "uhura.acme.usuario.info.started");
        assert_eq!(v["partitionkey"], "42");
        assert_eq!(v["facttype"], "EVENT");
        // campos None não são serializados
        assert!(v.get("subject").is_none());
    }

    #[test]
    fn roundtrips() {
        let e = Envelope::new("1", "svc", "uhura.acme.x.created");
        let s = serde_json::to_string(&e).unwrap();
        let back: Envelope = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, "1");
        assert_eq!(back.spec_version, CLOUDEVENTS_SPEC_VERSION);
    }
}
