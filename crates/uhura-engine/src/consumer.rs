//! Consumidor de demonstração (injeção/diagnóstico) com idempotência via Inbox.
//!
//! Fecha o loop de confiabilidade: lê da quorum queue do domínio, deduplica por
//! `envelope.id` (Inbox), faz `ack` no sucesso e `nack`→retry/parking no poison
//! (ver `SPEC.md` §10/§11/§12). Os consumidores reais são os SDKs; este existe
//! para validar o caminho fim-a-fim.

use futures_util::StreamExt;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use lapin::types::FieldTable;
use tokio_postgres::Client;
use uhura_core::{Envelope, Error, Result};
use uhura_pg::Inbox;
use uhura_transport::rabbitmq::RabbitMqTransport;
use uhura_transport::UhuraTransport;

/// Consumidor de um domínio.
pub struct Consumer {
    pub amqp_url: String,
    pub postgres_url: String,
    pub domain: String,
    /// Se presente, rejeita mensagens dessa partição (simula poison → parking).
    pub reject_partition: Option<String>,
}

impl Consumer {
    /// Consome até `Ctrl-C`.
    pub async fn run(self) -> Result<()> {
        let transport = RabbitMqTransport::connect(&self.amqp_url).await?;
        transport.ensure_topology(&self.domain).await?;
        let client = uhura_pg::connect(&self.postgres_url).await?;

        let queue = RabbitMqTransport::queue_name(&self.domain);
        let mut stream = transport
            .channel()
            .basic_consume(
                &queue,
                "uhura-cli",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("basic_consume {queue}: {e}")))?;

        tracing::info!(domain = %self.domain, queue = %queue, "consumindo (Ctrl-C para sair)");

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("sinal recebido, encerrando consumer");
                    break;
                }
                next = stream.next() => {
                    let Some(item) = next else { break };
                    let delivery = item.map_err(|e| Error::Transport(format!("delivery: {e}")))?;
                    self.handle(&client, &delivery).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle(&self, client: &Client, delivery: &Delivery) -> Result<()> {
        let envelope: Envelope = serde_json::from_slice(&delivery.data)
            .map_err(|e| Error::Transport(format!("parse envelope: {e}")))?;

        // Simulação de poison: rejeita e devolve para retry → parking.
        if self.reject_partition.is_some() && self.reject_partition == envelope.partitionkey {
            tracing::warn!(id = %envelope.id, "rejeitando (poison simulado) → retry/parking");
            delivery
                .nack(BasicNackOptions {
                    requeue: true,
                    multiple: false,
                })
                .await
                .map_err(|e| Error::Transport(format!("nack: {e}")))?;
            return Ok(());
        }

        // Idempotência: marca no Inbox antes de "processar".
        let inbox = Inbox::new(client);
        let is_new = inbox
            .mark_processed(&envelope.id, &self.domain, envelope.partitionkey.as_deref())
            .await?;
        if is_new {
            tracing::info!(id = %envelope.id, ty = %envelope.r#type, partition = ?envelope.partitionkey, "evento processado");
        } else {
            tracing::info!(id = %envelope.id, "duplicado ignorado (idempotência)");
        }

        delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|e| Error::Transport(format!("ack: {e}")))?;
        Ok(())
    }
}
