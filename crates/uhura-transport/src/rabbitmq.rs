//! Driver RabbitMQ do `UhuraTransport` (scaffold).
//!
//! Quorum queues + consistent-hash exchange + publisher confirms (ver `SPEC.md` §9/§10).

use async_trait::async_trait;
use uhura_core::{Envelope, Error, Result};

use crate::{PublishConfirm, UhuraTransport};

/// Transporte sobre RabbitMQ.
pub struct RabbitMqTransport {
    /// URL AMQP do broker.
    pub url: String,
}

impl RabbitMqTransport {
    /// Cria o driver apontando para `url` (conexão preguiçosa — TODO).
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[async_trait]
impl UhuraTransport for RabbitMqTransport {
    async fn ensure_topology(&self, domain: &str) -> Result<()> {
        tracing::debug!(broker = %self.url, domain, "ensure_topology (scaffold)");
        Err(Error::Unimplemented("rabbitmq: ensure_topology"))
    }

    async fn publish(&self, domain: &str, _envelope: &Envelope) -> Result<PublishConfirm> {
        tracing::debug!(broker = %self.url, domain, "publish (scaffold)");
        Err(Error::Unimplemented("rabbitmq: publish"))
    }
}
