//! `uhura-station` — engine de captura e despacho (scaffold).
//!
//! Único componente com estado operacional ativo: lê WAL/outbox, publica no
//! broker com confirms, faz leader-election (ver `SPEC.md` §13/§18).

use std::sync::Arc;

use uhura_core::{Error, Result, UhuraConfig};
use uhura_transport::UhuraTransport;

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

    /// Loop principal: (TODO) leader-election → WAL reader → dispatch → métricas.
    pub async fn run(self) -> Result<()> {
        tracing::info!(
            mesh = %self.config.mesh,
            amqp = %self.config.amqp_url,
            pg = %self.config.postgres_url,
            "uhura-station iniciando (scaffold)"
        );
        // TODO(SPEC §13/§18): leader-election, WAL logical decoding (pgoutput),
        // dispatch com publisher confirms + backpressure, métricas/observabilidade.
        let _ = self.transport.ensure_topology(&self.config.mesh).await;
        Err(Error::Unimplemented("station: run loop"))
    }
}
