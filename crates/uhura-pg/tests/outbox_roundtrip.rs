//! Teste de integração do outbox contra um Postgres real (testcontainers).
//!
//! Rode com: `cargo test -p uhura-pg --features integration`
//! Requer Docker disponível.
#![cfg(feature = "integration")]

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uhura_core::Envelope;
use uhura_pg::{connect, schema, Outbox};

#[tokio::test]
async fn outbox_insert_fetch_mark_roundtrip() {
    let node = Postgres::default().start().await.unwrap();
    let port = node.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // db init aplica o schema (idempotente).
    schema::init(&url).await.unwrap();

    let client = connect(&url).await.unwrap();
    let outbox = Outbox::new(&client);

    // Insere um evento.
    let mut env = Envelope::new("e1", "svc", "usuario.info.started");
    env.partitionkey = Some("42".to_string());
    let id = outbox
        .insert("usuario.info", "started", Some("42"), &env)
        .await
        .unwrap();

    // Aparece como não publicado.
    let batch = outbox.fetch_unpublished(10).await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, id);
    assert_eq!(batch[0].partitionkey.as_deref(), Some("42"));
    assert_eq!(batch[0].envelope.r#type, "usuario.info.started");

    // Marca publicado e some da varredura.
    assert_eq!(outbox.mark_published(&[id]).await.unwrap(), 1);
    assert!(outbox.fetch_unpublished(10).await.unwrap().is_empty());
}
