//! Captura de CDC via WAL logical decoding (alternativa ao trigger+polling).
//!
//! Mapeia mudanças de tabela (decodificadas do slot) para envelopes Uhura,
//! usando o mesmo mapeamento `.cdc` (tabela → contrato), e publica no broker
//! com publisher confirms. Ver `SPEC.md` §13.1.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use uhura_core::{Envelope, Error, FactType, Result};
use uhura_pg::wal::{self, Change};
use uhura_transport::UhuraTransport;

const SLOT: &str = "uhura_slot";
const BATCH: i32 = 256;
const IDLE_POLL: Duration = Duration::from_millis(250);

/// Mapeamento de uma tabela observada.
struct Mapping {
    contract: String,
    id_column: String,
}

/// Captura WAL: tail do slot → publish.
pub struct WalCapture {
    postgres_url: String,
    transport: Arc<dyn UhuraTransport>,
    mappings: HashMap<String, Mapping>,
}

impl WalCapture {
    /// Constrói a captura a partir dos arquivos `.cdc` (tabela → contrato).
    pub fn new(
        postgres_url: String,
        transport: Arc<dyn UhuraTransport>,
        cdc_dir: &str,
    ) -> Result<Self> {
        let specs = uhura_pg::schema::read_specs(cdc_dir)?;
        let mappings = specs
            .into_iter()
            .map(|s| {
                (
                    s.table,
                    Mapping {
                        contract: s.contract,
                        id_column: s.id_column,
                    },
                )
            })
            .collect();
        Ok(Self {
            postgres_url,
            transport,
            mappings,
        })
    }

    /// Loop de captura até `Ctrl-C`.
    pub async fn run(self) -> Result<()> {
        if self.mappings.is_empty() {
            return Err(Error::Storage(
                "nenhum mapeamento .cdc (tabela → contrato)".to_string(),
            ));
        }
        let client = uhura_pg::connect(&self.postgres_url).await?;
        wal::ensure_slot(&client, SLOT).await?;
        tracing::info!(slot = SLOT, tabelas = ?self.mappings.keys().collect::<Vec<_>>(), "captura WAL ativa");

        loop {
            let n = self.drain(&client).await?;
            if n == 0 {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("encerrando captura WAL");
                        break;
                    }
                    _ = tokio::time::sleep(IDLE_POLL) => {}
                }
            }
        }
        Ok(())
    }

    /// Lê um lote do slot, publica as mudanças mapeadas e avança o cursor.
    async fn drain(&self, client: &tokio_postgres::Client) -> Result<usize> {
        let rows = wal::peek_changes(client, SLOT, BATCH).await?;
        if rows.is_empty() {
            return Ok(0);
        }

        let mut published = 0usize;
        let mut last_lsn: Option<String> = None;
        for (lsn, data) in &rows {
            if let Some(change) = wal::parse_test_decoding(data) {
                if let Some(envelope_domain) = self.to_envelope(&change, lsn) {
                    let (domain, envelope) = envelope_domain;
                    let confirm = self.transport.publish(&domain, &envelope).await?;
                    if confirm.acked {
                        published += 1;
                    } else {
                        tracing::warn!("nack do broker; segurando o lote WAL");
                        break;
                    }
                }
            }
            last_lsn = Some(lsn.clone());
        }

        // Avança o slot só após publicar/confirmar (at-least-once).
        if let Some(lsn) = last_lsn {
            wal::advance(client, SLOT, &lsn).await?;
        }
        if published > 0 {
            tracing::info!(count = published, "mudanças WAL publicadas");
        }
        Ok(published)
    }

    /// Converte uma mudança mapeada em `(domínio, envelope)`.
    fn to_envelope(&self, change: &Change, lsn: &str) -> Option<(String, Envelope)> {
        let mapping = self.mappings.get(&change.table)?;
        let id = change.column(&mapping.id_column).unwrap_or("").to_string();
        let event = change.op.event();

        // id determinístico para dedup idempotente em reprocessamento.
        let env_id = format!("{SLOT}:{lsn}:{id}");
        let mut envelope = Envelope::new(
            env_id,
            format!("pg:{}", change.table),
            format!("{}.{}", mapping.contract, event),
        );
        envelope.time = Some(chrono::Utc::now());
        envelope.subject = Some(id.clone());
        envelope.partitionkey = Some(id);
        envelope.facttype = Some(FactType::Snapshot);
        let data: serde_json::Map<String, serde_json::Value> = change
            .columns
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        envelope.data = Some(serde_json::Value::Object(data));
        Some((mapping.contract.clone(), envelope))
    }
}
