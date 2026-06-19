//! `uhura-station` — engine de captura e despacho (MVP outbox + polling).
//!
//! Lê o `uhura_outbox` em ordem de id (commit), publica no broker com
//! publisher confirms e marca como publicado só após o `ack`. Para no primeiro
//! erro do lote para preservar ordem e aplicar backpressure (ver `SPEC.md`
//! §10/§11). WAL logical decoding entra depois sem mudar a ABI.

mod consumer;
pub use consumer::Consumer;

use std::sync::Arc;
use std::time::Duration;

use uhura_core::{Result, UhuraConfig};
use uhura_pg::Outbox;
use uhura_transport::UhuraTransport;

/// Tamanho do lote lido do outbox por ciclo.
const BATCH: i64 = 128;
/// Intervalo de polling quando o outbox está vazio (fallback do LISTEN/NOTIFY).
const IDLE_POLL: Duration = Duration::from_millis(250);

/// Engine de instrumentação do bus.
pub struct Station {
    config: UhuraConfig,
    transport: Arc<dyn UhuraTransport>,
}

impl Station {
    /// Constrói a station com sua configuração e driver de transporte.
    pub fn new(config: UhuraConfig, transport: Arc<dyn UhuraTransport>) -> Self {
        Self { config, transport }
    }

    /// Loop principal de dispatch até `Ctrl-C`.
    pub async fn run(self) -> Result<()> {
        let client = uhura_pg::connect(&self.config.postgres_url).await?;
        let outbox = Outbox::new(&client);

        tracing::info!(
            mesh = %self.config.mesh,
            amqp = %self.config.amqp_url,
            "uhura-station ativo (outbox + polling)"
        );

        loop {
            let dispatched = self.dispatch_batch(&outbox).await?;
            if dispatched == 0 {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("sinal recebido, encerrando uhura-station");
                        break;
                    }
                    _ = tokio::time::sleep(IDLE_POLL) => {}
                }
            }
        }
        Ok(())
    }

    /// Publica um lote do outbox; para no primeiro insucesso (ordem/backpressure).
    async fn dispatch_batch(&self, outbox: &Outbox<'_>) -> Result<usize> {
        let batch = outbox.fetch_unpublished(BATCH).await?;
        if batch.is_empty() {
            return Ok(0);
        }

        let mut published = Vec::with_capacity(batch.len());
        for rec in &batch {
            match self.transport.publish(&rec.domain, &rec.envelope).await {
                Ok(c) if c.acked => published.push(rec.id),
                Ok(_) => {
                    tracing::warn!(id = rec.id, "nack do broker; segurando o restante do lote");
                    break;
                }
                Err(e) => {
                    tracing::warn!(id = rec.id, error = %e, "falha ao publicar; segurando o lote");
                    break;
                }
            }
        }

        let n = published.len();
        outbox.mark_published(&published).await?;
        if n > 0 {
            tracing::info!(count = n, "eventos publicados e confirmados");
        }
        Ok(n)
    }
}
