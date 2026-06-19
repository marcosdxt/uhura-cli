//! Operações de schema (`uhura db init` / `uhura db sync`).

use uhura_core::{Error, Result};

/// DDL idempotente do schema base (outbox/inbox + trigger de NOTIFY).
const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");

/// Cria/atualiza as tabelas `uhura_outbox` e `uhura_inbox` e o trigger de notify.
pub async fn init(postgres_url: &str) -> Result<()> {
    let client = crate::connect(postgres_url).await?;
    client
        .batch_execute(SCHEMA_SQL)
        .await
        .map_err(|e| Error::Storage(format!("db init: {e}")))?;
    tracing::info!("schema Uhura aplicado (uhura_outbox, uhura_inbox, trigger notify)");
    Ok(())
}

/// Gera/aplica migrations de triggers de entidade a partir dos arquivos `.cdc`.
pub async fn sync(postgres_url: &str, cdc_dir: &str) -> Result<()> {
    tracing::debug!(url = %postgres_url, cdc = %cdc_dir, "db sync (scaffold)");
    Err(Error::Unimplemented("db: sync (.cdc migrations)"))
}
