//! Binário `uhura` — CLI de operação do bus e host do engine `uhura-station`.

mod cli;
mod commands;
mod serve;

use clap::Parser;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.debug);
    commands::dispatch(cli).await
}

/// Configura logs estruturados; `--debug` eleva o nível e o detalhe.
fn init_tracing(debug: bool) {
    use tracing_subscriber::{fmt, EnvFilter};

    let default = if debug {
        "uhura=debug,info"
    } else {
        "uhura=info,warn"
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    fmt().with_env_filter(filter).with_target(false).init();
}
