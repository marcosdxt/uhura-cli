//! Driver RabbitMQ do `UhuraTransport` (MVP).
//!
//! Por domínio declara: exchange durável (`topic`), quorum queue de consumo com
//! DLX → parking, e exchange/queue de parking. Publica em modo *structured*
//! CloudEvents (`application/cloudevents+json`) com **publisher confirms** e
//! `delivery_mode=2` (persistente). Ver `SPEC.md` §9/§10.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::publisher_confirm::Confirmation;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::Mutex;
use uhura_core::{Envelope, Error, Result, RpcRequest, RpcResult};

use crate::{PublishConfirm, UhuraTransport};

/// Pseudo-fila de resposta direta do RabbitMQ (RPC sem declarar fila de reply).
const DIRECT_REPLY_TO: &str = "amq.rabbitmq.reply-to";

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

    /// Acesso ao canal (para consumidores que vivem fora do driver).
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn exchange_name(domain: &str) -> String {
        format!("uhura.{domain}")
    }
    pub fn queue_name(domain: &str) -> String {
        format!("uhura.{domain}.q")
    }
    pub fn parking_exchange(domain: &str) -> String {
        format!("uhura.{domain}.parking")
    }
    pub fn parking_queue(domain: &str) -> String {
        format!("uhura.{domain}.parking.q")
    }
    pub fn rpc_queue_name(domain: &str) -> String {
        format!("uhura.{domain}.rpc")
    }

    /// Nº de mensagens prontas numa fila (via declare passivo).
    async fn queue_message_count(&self, queue: &str) -> Result<u32> {
        let q = self
            .channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("declare passivo {queue}: {e}")))?;
        Ok(q.message_count())
    }

    /// Contagem (main, parking) do domínio.
    pub async fn depths(&self, domain: &str) -> Result<(u32, u32)> {
        self.ensure_topology(domain).await?;
        let main = self.queue_message_count(&Self::queue_name(domain)).await?;
        let parking = self
            .queue_message_count(&Self::parking_queue(domain))
            .await?;
        Ok((main, parking))
    }

    /// Reenvia (replay) todas as mensagens do parking de volta à exchange do domínio.
    pub async fn parking_replay(&self, domain: &str) -> Result<usize> {
        self.ensure_topology(domain).await?;
        let parking_q = Self::parking_queue(domain);
        let mut count = 0usize;
        loop {
            let got = self
                .channel
                .basic_get(&parking_q, BasicGetOptions { no_ack: false })
                .await
                .map_err(|e| Error::Transport(format!("basic_get parking: {e}")))?;
            let Some(msg) = got else { break };
            let envelope: Envelope = serde_json::from_slice(&msg.delivery.data)
                .map_err(|e| Error::Transport(format!("parse mensagem do parking: {e}")))?;
            self.publish(domain, &envelope).await?;
            msg.delivery
                .ack(BasicAckOptions::default())
                .await
                .map_err(|e| Error::Transport(format!("ack parking: {e}")))?;
            count += 1;
        }
        Ok(count)
    }

    /// Cliente RPC: envia uma requisição e aguarda o `RpcResult` (direct reply-to).
    pub async fn rpc_call(
        &self,
        domain: &str,
        method: &str,
        data: serde_json::Value,
        timeout: Duration,
    ) -> Result<RpcResult<serde_json::Value>> {
        let request_queue = Self::rpc_queue_name(domain);
        // Declara a fila de requisição (quorum), compatível com o servidor.
        self.channel
            .queue_declare(
                &request_queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                quorum_args(None),
            )
            .await
            .map_err(|e| Error::Transport(format!("declare rpc queue: {e}")))?;

        // Consome a pseudo-fila de resposta direta.
        let mut replies = self
            .channel
            .basic_consume(
                DIRECT_REPLY_TO,
                "uhura-rpc-client",
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Transport(format!("consume reply-to: {e}")))?;

        let correlation = uuid::Uuid::new_v4().to_string();
        let request = RpcRequest {
            id: correlation.clone(),
            domain: domain.to_string(),
            method: method.to_string(),
            data,
        };
        let body = serde_json::to_vec(&request)
            .map_err(|e| Error::Transport(format!("serialização da requisição: {e}")))?;
        let props = BasicProperties::default()
            .with_correlation_id(correlation.clone().into())
            .with_reply_to(DIRECT_REPLY_TO.into())
            .with_content_type("application/json".into());

        self.channel
            .basic_publish(
                "",
                &request_queue,
                BasicPublishOptions::default(),
                &body,
                props,
            )
            .await
            .map_err(|e| Error::Transport(format!("publicação RPC: {e}")))?;

        let wait = async {
            while let Some(delivery) = replies.next().await {
                let delivery = delivery.map_err(|e| Error::Transport(format!("reply: {e}")))?;
                let matches = delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .map(|c| c.as_str())
                    == Some(correlation.as_str());
                if matches {
                    let result: RpcResult<serde_json::Value> =
                        serde_json::from_slice(&delivery.data).map_err(|e| {
                            Error::Transport(format!("desserialização do RpcResult: {e}"))
                        })?;
                    return Ok(result);
                }
            }
            Err(Error::Transport("stream de reply encerrado".to_string()))
        };

        match tokio::time::timeout(timeout, wait).await {
            Ok(result) => result,
            Err(_) => Err(Error::Transport(format!("timeout RPC após {timeout:?}"))),
        }
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
