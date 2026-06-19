//! Definição da árvore de comandos (clap). Espelha `SPEC.md` §14.

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "uhura",
    version,
    about = "Uhura message bus — CLI e engine (uhura-station)",
    long_about = None
)]
pub struct Cli {
    /// Logging detalhado e tracing granular.
    #[arg(long, global = true)]
    pub debug: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Codegen de contratos (tipos NestJS/Rust), docs e compat-check.
    Sync(SyncArgs),
    /// Operações de banco (slot/WAL, outbox/inbox, triggers, migrations `.cdc`).
    Db {
        #[command(subcommand)]
        cmd: DbCmd,
    },
    /// Sobe o engine uhura-station (WAL reader + dispatcher + métricas).
    Station(StationArgs),
    /// Cria/valida a topologia RabbitMQ (exchanges, quorum queues, DLX, parking).
    Topology {
        #[command(subcommand)]
        cmd: TopologyCmd,
    },
    /// TUI de monitoração (filas, lag de slot, mensagens paradas).
    Top,
    /// Parking lot: listar e reenviar mensagens.
    Parking {
        #[command(subcommand)]
        cmd: ParkingCmd,
    },
    /// Publica um evento de contrato (injeção de primitiva).
    Publish(PublishArgs),
    /// Chama um método RPC (injeção de primitiva).
    Method(MethodArgs),
    /// Gera/serve a documentação de contratos e serviços.
    Doc(DocArgs),
}

/// Linguagem-alvo do codegen.
#[derive(Debug, Clone, ValueEnum)]
pub enum Target {
    Nestjs,
    Rust,
    Both,
}

/// Modo de captura de mudanças do engine.
#[derive(Debug, Clone, ValueEnum)]
pub enum CaptureMode {
    /// WAL logical decoding (preferencial).
    Wal,
    /// Trigger + outbox polling (compatível).
    Poll,
}

#[derive(Debug, Args)]
pub struct SyncArgs {
    /// Diretório do submódulo de contratos.
    #[arg(long, default_value = "./contracts")]
    pub contracts: String,
    /// Diretório de saída do codegen.
    #[arg(long, default_value = "./generated")]
    pub out: String,
    /// Linguagem-alvo.
    #[arg(long, default_value = "both")]
    pub target: Target,
}

#[derive(Debug, Subcommand)]
pub enum DbCmd {
    /// Cria `wal_level`/slot e tabelas outbox/inbox.
    Init(DbConnArgs),
    /// Aplica migrations a partir de arquivos `.cdc`.
    Sync(DbConnArgs),
}

#[derive(Debug, Args)]
pub struct DbConnArgs {
    /// URL do PostgreSQL.
    #[arg(long, env = "UHURA_PG_URL")]
    pub postgres_url: Option<String>,
    /// Diretório com arquivos `.cdc`.
    #[arg(long, default_value = "./cdc")]
    pub cdc: String,
}

#[derive(Debug, Args)]
pub struct StationArgs {
    /// URL AMQP do RabbitMQ.
    #[arg(long, env = "UHURA_AMQP_URL")]
    pub amqp_url: Option<String>,
    /// URL do PostgreSQL.
    #[arg(long, env = "UHURA_PG_URL")]
    pub postgres_url: Option<String>,
    /// Nome do mesh.
    #[arg(long, env = "UHURA_MESH")]
    pub mesh: Option<String>,
    /// Modo de captura: `wal` (preferencial) ou `poll` (compatível).
    #[arg(long, default_value = "wal")]
    pub capture: CaptureMode,
}

#[derive(Debug, Subcommand)]
pub enum TopologyCmd {
    /// Aplica/valida a topologia.
    Apply(TopologyArgs),
}

#[derive(Debug, Args)]
pub struct TopologyArgs {
    /// URL AMQP do RabbitMQ.
    #[arg(long, env = "UHURA_AMQP_URL")]
    pub amqp_url: Option<String>,
    /// Apenas valida, sem aplicar.
    #[arg(long)]
    pub check: bool,
}

#[derive(Debug, Subcommand)]
pub enum ParkingCmd {
    /// Lista mensagens no parking lot.
    List(ParkingArgs),
    /// Reenvia (replay) mensagens do parking lot.
    Replay(ParkingArgs),
}

#[derive(Debug, Args)]
pub struct ParkingArgs {
    /// Restringe a um domínio.
    #[arg(long)]
    pub domain: Option<String>,
    /// URL AMQP do RabbitMQ.
    #[arg(long, env = "UHURA_AMQP_URL")]
    pub amqp_url: Option<String>,
}

#[derive(Debug, Args)]
pub struct PublishArgs {
    /// Domínio do contrato (ex.: `usuario.info`).
    pub domain: String,
    /// Evento (ex.: `started`).
    pub event: String,
    /// Payload JSON.
    #[arg(long)]
    pub data: String,
}

#[derive(Debug, Args)]
pub struct MethodArgs {
    /// Domínio do contrato.
    pub domain: String,
    /// Nome do método.
    pub method: String,
    /// Payload JSON.
    #[arg(long)]
    pub data: String,
}

#[derive(Debug, Args)]
pub struct DocArgs {
    /// Diretório do submódulo de contratos.
    #[arg(long, default_value = "./contracts")]
    pub contracts: String,
    /// Diretório de saída da documentação.
    #[arg(long, default_value = "./doc")]
    pub out: String,
    /// Sobe um servidor local para navegar a doc.
    #[arg(long)]
    pub serve: bool,
}
