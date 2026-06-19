//! Despacho dos subcomandos da CLI (scaffold).
//!
//! Cada handler já está cabeado às camadas (`uhura-codec`, `uhura-pg`,
//! `uhura-engine`, `uhura-transport`); as implementações retornam
//! `Error::Unimplemented` por enquanto.

use std::sync::Arc;

use crate::cli::*;

/// Roteia o comando parseado para o handler correspondente.
pub async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Sync(a) => cmd_sync(a),
        Command::Db { cmd } => cmd_db(cmd).await,
        Command::Station(a) => cmd_station(a).await,
        Command::Topology { cmd } => cmd_topology(cmd),
        Command::Top => cmd_top(),
        Command::Parking { cmd } => cmd_parking(cmd),
        Command::Publish(a) => cmd_publish(a),
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
    let transport = Arc::new(uhura_transport::rabbitmq::RabbitMqTransport::new(
        config.amqp_url.clone(),
    ));
    let station = uhura_engine::Station::new(config, transport);
    report(station.run().await)
}

fn cmd_topology(cmd: TopologyCmd) -> anyhow::Result<()> {
    match cmd {
        TopologyCmd::Apply(a) => {
            tracing::info!(amqp = ?a.amqp_url, check = a.check, "uhura topology apply");
            report(Err(uhura_core::Error::Unimplemented("topology: apply")))
        }
    }
}

fn cmd_top() -> anyhow::Result<()> {
    report(Err(uhura_core::Error::Unimplemented(
        "top: TUI de monitoração",
    )))
}

fn cmd_parking(cmd: ParkingCmd) -> anyhow::Result<()> {
    let (action, a) = match cmd {
        ParkingCmd::List(a) => ("list", a),
        ParkingCmd::Replay(a) => ("replay", a),
    };
    tracing::info!(action, domain = ?a.domain, amqp = ?a.amqp_url, "uhura parking");
    report(Err(uhura_core::Error::Unimplemented(
        "parking: list/replay",
    )))
}

fn cmd_publish(a: PublishArgs) -> anyhow::Result<()> {
    tracing::info!(domain = %a.domain, event = %a.event, data = %a.data, "uhura publish");
    report(Err(uhura_core::Error::Unimplemented("publish")))
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
