//! Camada L2 (armazenamento) — outbox/inbox e schema no PostgreSQL (scaffold).
//!
//! Postgres é a única fonte durável de verdade (ver `SPEC.md` §2/§13).

use async_trait::async_trait;
use uhura_core::{Envelope, Result};

/// Cursor durável da leitura (LSN no modo WAL, id no modo polling).
pub type Cursor = String;

/// Leitor de eventos a publicar — implementado por WAL (preferencial) ou polling.
#[async_trait]
pub trait OutboxReader: Send + Sync {
    /// Próximo lote de envelopes a despachar, em ordem de commit.
    async fn next_batch(&mut self, max: usize) -> Result<Vec<Envelope>>;

    /// Persiste o cursor após confirmação do broker.
    async fn commit_cursor(&mut self, up_to: &Cursor) -> Result<()>;
}

/// Operações de schema (`uhura db init` / `uhura db sync`).
pub mod schema {
    use uhura_core::{Error, Result};

    /// Cria `wal_level`/replication slot e tabelas `uhura_outbox`/`uhura_inbox`.
    pub async fn init(postgres_url: &str) -> Result<()> {
        tracing::debug!(target: "uhura_pg", url = %postgres_url, "db init (scaffold)");
        Err(Error::Unimplemented("db: init"))
    }

    /// Gera/aplica migrations a partir dos arquivos `.cdc`.
    pub async fn sync(postgres_url: &str, cdc_dir: &str) -> Result<()> {
        tracing::debug!(target: "uhura_pg", url = %postgres_url, cdc = %cdc_dir, "db sync (scaffold)");
        Err(Error::Unimplemented("db: sync"))
    }
}
