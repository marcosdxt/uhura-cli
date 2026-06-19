//! Camada L1 — interface de transporte plugável (`UhuraTransport`).
//!
//! RabbitMQ é o primeiro driver (ver `SPEC.md` §9). O núcleo não conhece
//! detalhes do broker.

use async_trait::async_trait;
use uhura_core::{Envelope, Result};

pub mod rabbitmq;

/// Resultado de uma publicação confirmada pelo broker (publisher confirms).
#[derive(Debug, Clone)]
pub struct PublishConfirm {
    /// Tag de entrega atribuída pelo canal.
    pub delivery_tag: u64,
    /// `true` = `ack`, `false` = `nack` do broker.
    pub acked: bool,
}

/// Driver de transporte do bus.
#[async_trait]
pub trait UhuraTransport: Send + Sync {
    /// Garante exchange/filas/bindings do domínio (idempotente).
    async fn ensure_topology(&self, domain: &str) -> Result<()>;

    /// Publica um envelope e retorna a confirmação do broker.
    async fn publish(&self, domain: &str, envelope: &Envelope) -> Result<PublishConfirm>;
}
