//! Integração do CDC trigger-based contra Postgres real (testcontainers).
//!
//! `cargo test -p uhura-pg --features integration` (requer Docker).
#![cfg(feature = "integration")]

use std::fs;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uhura_pg::{connect, schema};

#[tokio::test]
async fn cdc_trigger_writes_outbox() {
    // Baseline PG ≥ 16 (gen_random_uuid em core).
    let node = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .unwrap();
    let port = node.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    schema::init(&url).await.unwrap();
    let client = connect(&url).await.unwrap();
    client
        .batch_execute("CREATE TABLE conta (id text PRIMARY KEY, saldo numeric)")
        .await
        .unwrap();

    // .cdc → db sync gera o trigger.
    let dir = std::env::temp_dir().join(format!("uhura-cdc-{port}"));
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("conta.cdc"),
        r#"[{ "table": "conta", "events": ["inserted","updated"], "id_column": "id", "contract": "banco.conta" }]"#,
    )
    .unwrap();
    schema::sync(&url, dir.to_str().unwrap()).await.unwrap();

    // Muta a tabela → o trigger escreve no outbox.
    client
        .batch_execute(
            "INSERT INTO conta VALUES ('a1', 10); UPDATE conta SET saldo = 20 WHERE id = 'a1'",
        )
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT domain, event, partitionkey FROM uhura_outbox ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "INSERT + UPDATE devem gerar 2 eventos");

    let domain: String = rows[0].get("domain");
    let event0: String = rows[0].get("event");
    let pk0: String = rows[0].get("partitionkey");
    assert_eq!(domain, "banco.conta");
    assert_eq!(event0, "inserted");
    assert_eq!(pk0, "a1");

    let event1: String = rows[1].get("event");
    assert_eq!(event1, "updated");

    fs::remove_dir_all(&dir).ok();
}
