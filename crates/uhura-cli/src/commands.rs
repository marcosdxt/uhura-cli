//! Despacho dos subcomandos da CLI (scaffold).
//!
//! Cada handler já está cabeado às camadas (`uhura-codec`, `uhura-pg`,
//! `uhura-engine`, `uhura-transport`); as implementações retornam
//! `Error::Unimplemented` por enquanto.

use std::sync::Arc;

use uhura_transport::UhuraTransport;

use crate::cli::*;

/// Roteia o comando parseado para o handler correspondente.
pub async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Sync(a) => cmd_sync(a),
        Command::Db { cmd } => cmd_db(cmd).await,
        Command::Station(a) => cmd_station(a).await,
        Command::Topology { cmd } => cmd_topology(cmd).await,
        Command::Top(a) => cmd_top(a).await,
        Command::Parking { cmd } => cmd_parking(cmd).await,
        Command::Publish(a) => cmd_publish(a).await,
        Command::Consume(a) => cmd_consume(a).await,
        Command::Method(a) => cmd_method(a),
        Command::Doc(a) => cmd_doc(a),
    }
}

/// Converte um `uhura_core::Result` em saída amigável de scaffold.
fn report(r: uhura_core::Result<()>) -> anyhow::Result<()> {
    match r {
        Ok(()) => Ok(()),
        Err(uhura_core::Error::Unimplemented(what)) => {
            println!("uhura: '{what}' — scaffold, ainda não implementado (ver SPEC.md).");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn cmd_sync(a: SyncArgs) -> anyhow::Result<()> {
    tracing::info!(contracts = %a.contracts, out = %a.out, target = ?a.target, "uhura sync");
    report(uhura_codec::sync(&a.contracts, &a.out))
}

async fn cmd_db(cmd: DbCmd) -> anyhow::Result<()> {
    match cmd {
        DbCmd::Init(c) => {
            let url = c.postgres_url.unwrap_or_else(default_pg);
            report(uhura_pg::schema::init(&url).await)
        }
        DbCmd::Sync(c) => {
            let url = c.postgres_url.unwrap_or_else(default_pg);
            report(uhura_pg::schema::sync(&url, &c.cdc).await)
        }
    }
}

async fn cmd_station(a: StationArgs) -> anyhow::Result<()> {
    let config = uhura_core::UhuraConfig {
        amqp_url: a.amqp_url.unwrap_or_else(default_amqp),
        postgres_url: a.postgres_url.unwrap_or_else(default_pg),
        mesh: a.mesh.unwrap_or_else(default_mesh),
        debug: false,
    };
    tracing::info!(capture = ?a.capture, "iniciando uhura-station");
    let transport = uhura_transport::rabbitmq::RabbitMqTransport::connect(&config.amqp_url).await?;
    let station = uhura_engine::Station::new(config, Arc::new(transport));
    station.run().await.map_err(anyhow::Error::from)
}

async fn cmd_topology(cmd: TopologyCmd) -> anyhow::Result<()> {
    match cmd {
        TopologyCmd::Apply(a) => {
            let url = a.amqp_url.unwrap_or_else(default_amqp);
            tracing::info!(domains = ?a.domains, check = a.check, "uhura topology apply");
            let transport = uhura_transport::rabbitmq::RabbitMqTransport::connect(&url).await?;
            if a.domains.is_empty() {
                println!("uhura: informe ao menos um --domain.");
                return Ok(());
            }
            for domain in &a.domains {
                if a.check {
                    println!("uhura: (check) conexão OK — topologia de '{domain}' não aplicada.");
                } else {
                    transport.ensure_topology(domain).await?;
                    println!("uhura: topologia aplicada para '{domain}'.");
                }
            }
            Ok(())
        }
    }
}

async fn cmd_top(a: TopArgs) -> anyhow::Result<()> {
    let url = a.amqp_url.unwrap_or_else(default_amqp);
    if a.domains.is_empty() {
        println!("uhura: informe ao menos um --domain.");
        return Ok(());
    }
    let transport = uhura_transport::rabbitmq::RabbitMqTransport::connect(&url).await?;
    println!("{:<32} {:>8} {:>8}", "DOMÍNIO", "MAIN", "PARKING");
    for domain in &a.domains {
        let (main, parking) = transport.depths(domain).await?;
        println!("{domain:<32} {main:>8} {parking:>8}");
    }
    Ok(())
}

async fn cmd_parking(cmd: ParkingCmd) -> anyhow::Result<()> {
    let (replay, a) = match cmd {
        ParkingCmd::List(a) => (false, a),
        ParkingCmd::Replay(a) => (true, a),
    };
    let domain = a
        .domain
        .ok_or_else(|| anyhow::anyhow!("--domain é obrigatório"))?;
    let url = a.amqp_url.unwrap_or_else(default_amqp);
    let transport = uhura_transport::rabbitmq::RabbitMqTransport::connect(&url).await?;
    if replay {
        let n = transport.parking_replay(&domain).await?;
        println!("uhura: {n} mensagem(ns) reenviada(s) do parking de '{domain}'.");
    } else {
        let (_, parking) = transport.depths(&domain).await?;
        println!("uhura: parking de '{domain}' tem {parking} mensagem(ns).");
    }
    Ok(())
}

async fn cmd_consume(a: ConsumeArgs) -> anyhow::Result<()> {
    let consumer = uhura_engine::Consumer {
        amqp_url: a.amqp_url.unwrap_or_else(default_amqp),
        postgres_url: a.postgres_url.unwrap_or_else(default_pg),
        domain: a.domain,
        reject_partition: a.reject,
    };
    consumer.run().await.map_err(anyhow::Error::from)
}

async fn cmd_publish(a: PublishArgs) -> anyhow::Result<()> {
    tracing::info!(domain = %a.domain, event = %a.event, "uhura publish");

    let data: serde_json::Value = serde_json::from_str(&a.data)
        .map_err(|e| anyhow::anyhow!("--data não é JSON válido: {e}"))?;

    // Monta o envelope CloudEvents 1.0 (ver SPEC.md §7).
    let mut envelope = uhura_core::Envelope::new(
        uuid::Uuid::new_v4().to_string(),
        a.source.clone(),
        format!("{}.{}", a.domain, a.event),
    );
    envelope.time = Some(chrono::Utc::now());
    envelope.subject = a.partition.clone();
    envelope.partitionkey = a.partition.clone();
    envelope.facttype = Some(uhura_core::FactType::Event);
    envelope.data = Some(data);

    let url = a.postgres_url.unwrap_or_else(default_pg);
    let client = uhura_pg::connect(&url).await?;
    let outbox = uhura_pg::Outbox::new(&client);
    let id = outbox
        .insert(&a.domain, &a.event, a.partition.as_deref(), &envelope)
        .await?;

    println!(
        "uhura: evento gravado no outbox (id={id}, type={}).",
        envelope.r#type
    );
    Ok(())
}

fn cmd_method(a: MethodArgs) -> anyhow::Result<()> {
    tracing::info!(domain = %a.domain, method = %a.method, data = %a.data, "uhura method");
    report(Err(uhura_core::Error::Unimplemented("method")))
}

fn cmd_doc(a: DocArgs) -> anyhow::Result<()> {
    tracing::info!(contracts = %a.contracts, out = %a.out, serve = a.serve, "uhura doc");
    report(uhura_codec::generate_docs(&a.contracts, &a.out))
}

fn default_pg() -> String {
    "postgres://localhost/uhura".to_string()
}
fn default_amqp() -> String {
    "amqp://127.0.0.1:5672".to_string()
}
fn default_mesh() -> String {
    "default".to_string()
}
