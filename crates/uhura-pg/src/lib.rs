//! Camada L2 (armazenamento) — outbox/inbox e schema no PostgreSQL.
//!
//! Postgres é a única fonte durável de verdade (ver `SPEC.md` §2/§13).
//! O MVP usa o caminho **outbox + polling**; o WAL logical decoding entra depois
//! sem mudar a ABI nem o Inbox.

mod conn;
mod inbox;
mod outbox;
pub mod schema;

pub use conn::connect;
pub use inbox::Inbox;
pub use outbox::{Outbox, OutboxRecord};

use async_trait::async_trait;
use uhura_core::{Envelope, Result};

/// Cursor durável da leitura (LSN no modo WAL, id no modo polling).
pub type Cursor = String;

/// Leitor de eventos a publicar — implementado por WAL (preferencial) ou polling.
///
/// Abstração-alvo para o engine; o MVP usa [`Outbox`] diretamente.
#[async_trait]
pub trait OutboxReader: Send + Sync {
    /// Próximo lote de envelopes a despachar, em ordem de commit.
    async fn next_batch(&mut self, max: usize) -> Result<Vec<Envelope>>;

    /// Persiste o cursor após confirmação do broker.
    async fn commit_cursor(&mut self, up_to: &Cursor) -> Result<()>;
}
