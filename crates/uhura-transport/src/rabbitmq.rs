//! Driver RabbitMQ do `UhuraTransport` (MVP).
//!
//! Por domínio declara: exchange durável (`topic`), quorum queue de consumo com
//! DLX → parking, e exchange/queue de parking. Publica em modo *structured*
//! CloudEvents (`application/cloudevents+json`) com **publisher confirms** e
//! `delivery_mode=2` (persistente). Ver `SPEC.md` §9/§10.

use std::collections::HashSet;

use async_trait::async_trait;
use lapin::options::{
    BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::publisher_confirm::Confirmation;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::Mutex;
use uhura_core::{Envelope, Error, Result};

use crate::{PublishConfirm, UhuraTransport};

/// Limite de entregas antes de mandar ao parking (poison-message handling).
const DELIVERY_LIMIT: i32 = 5;

/// Transporte sobre RabbitMQ com canal em modo confirm.
pub struct RabbitMqTransport {
    channel: Channel,
    _conn: Connection,
    declared: Mutex<HashSet<String>>,
}

impl RabbitMqTransport {
    /// Abre conexão + canal e ativa publisher confirms.
    pub async fn connect(url: &str) -> Result<Self> {
        let conn = Connection::connect(url, ConnectionProperties::default())
            .await
            .map_err(|e| Error::Transport(format!("conexão AMQP: {e}")))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| Error::Transport(format!("canal AMQP: {e}")))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| Error::Transport(format!("confirm_select: {e}")))?;
        Ok(Self {
            channel,
            _conn: conn,
            declared: Mutex::new(HashSet::new()),
        })
    }

    fn exchange_name(domain: &str) -> String {
        format!("uhura.{domain}")
    }
    fn queue_name(domain: &str) -> String {
        format!("uhura.{domain}.q")
    }
    fn parking_exchange(domain: &str) -> String {
        format!("uhura.{domain}.parking")
    }
    fn parking_queue(domain: &str) -> String {
        format!("uhura.{domain}.parking.q")
    }

    async fn declare_topology(&self, domain: &str) -> Result<()> {
        let durable = ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        };
        let durable_q = QueueDeclareOptions {
            durable: true,
            ..Default::default()
        };

        let exchange = Self::exchange_name(domain);
        let parking_ex = Self::parking_exchange(domain);
        let parking_q = Self::parking_queue(domain);
        let main_q = Self::queue_name(domain);

        // Exchange de domínio.
        self.channel
            .exchange_declare(
                &exchange,
                ExchangeKind::Topic,
                durable,
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("declare exchange {exchange}: {e}")))?;

        // Parking: exchange fanout + quorum queue.
        self.channel
            .exchange_declare(
                &parking_ex,
                ExchangeKind::Fanout,
                durable,
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("declare parking exchange: {e}")))?;
        self.channel
            .queue_declare(&parking_q, durable_q, quorum_args(None))
            .await
            .map_err(|e| Error::Transport(format!("declare parking queue: {e}")))?;
        self.channel
            .queue_bind(
                &parking_q,
                &parking_ex,
                "",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("bind parking queue: {e}")))?;

        // Quorum queue principal com DLX → parking.
        self.channel
            .queue_declare(&main_q, durable_q, quorum_args(Some(&parking_ex)))
            .await
            .map_err(|e| Error::Transport(format!("declare queue {main_q}: {e}")))?;
        self.channel
            .queue_bind(
                &main_q,
                &exchange,
                "#",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("bind queue {main_q}: {e}")))?;

        Ok(())
    }
}

/// Argumentos de uma quorum queue, opcionalmente com DLX.
fn quorum_args(dead_letter_exchange: Option<&str>) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("quorum".into()),
    );
    if let Some(dlx) = dead_letter_exchange {
        args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(dlx.into()),
        );
        args.insert(
            "x-delivery-limit".into(),
            AMQPValue::LongInt(DELIVERY_LIMIT),
        );
    }
    args
}

#[async_trait]
impl UhuraTransport for RabbitMqTransport {
    async fn ensure_topology(&self, domain: &str) -> Result<()> {
        {
            let declared = self.declared.lock().await;
            if declared.contains(domain) {
                return Ok(());
            }
        }
        self.declare_topology(domain).await?;
        self.declared.lock().await.insert(domain.to_string());
        tracing::debug!(domain, "topologia declarada");
        Ok(())
    }

    async fn publish(&self, domain: &str, envelope: &Envelope) -> Result<PublishConfirm> {
        self.ensure_topology(domain).await?;

        let body = serde_json::to_vec(envelope)
            .map_err(|e| Error::Transport(format!("serialização do envelope: {e}")))?;
        let routing_key = envelope
            .partitionkey
            .clone()
            .unwrap_or_else(|| "default".into());
        let props = BasicProperties::default()
            .with_delivery_mode(2) // persistente
            .with_content_type("application/cloudevents+json".into())
            .with_message_id(envelope.id.clone().into());

        let confirm = self
            .channel
            .basic_publish(
                &Self::exchange_name(domain),
                &routing_key,
                BasicPublishOptions::default(),
                &body,
                props,
            )
            .await
            .map_err(|e| Error::Transport(format!("basic_publish: {e}")))?
            .await
            .map_err(|e| Error::Transport(format!("confirmação: {e}")))?;

        let acked = matches!(confirm, Confirmation::Ack(_) | Confirmation::NotRequested);
        Ok(PublishConfirm {
            delivery_tag: 0,
            acked,
        })
    }
}
