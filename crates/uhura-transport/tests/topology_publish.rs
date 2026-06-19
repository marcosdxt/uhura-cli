//! Integração do driver RabbitMQ contra um broker real (testcontainers).
//!
//! Rode com: `cargo test -p uhura-transport --features integration`
//! Requer Docker disponível.
#![cfg(feature = "integration")]

use std::time::Duration;

use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uhura_core::Envelope;
use uhura_transport::rabbitmq::RabbitMqTransport;
use uhura_transport::UhuraTransport;

#[tokio::test]
async fn publish_routes_to_quorum_queue() {
    let node = RabbitMq::default().start().await.unwrap();
    let port = node.get_host_port_ipv4(5672).await.unwrap();
    let url = format!("amqp://guest:guest@127.0.0.1:{port}");

    let transport = RabbitMqTransport::connect(&url).await.unwrap();
    let domain = "teste.evento";

    let mut env = Envelope::new("e1", "svc", "teste.evento.created");
    env.partitionkey = Some("k1".to_string());

    // publish declara a topologia e confirma a entrega.
    let confirm = transport.publish(domain, &env).await.unwrap();
    assert!(confirm.acked, "broker deve confirmar (ack)");

    // pequena folga para a estatística da quorum queue refletir.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (main, parking) = transport.depths(domain).await.unwrap();
    assert_eq!(main, 1, "mensagem deve estar na quorum queue principal");
    assert_eq!(parking, 0, "parking deve estar vazio");
}
