//! Métricas agregadas para o backend do console (`uhura serve`).

use tokio_postgres::Client;
use uhura_core::{Error, Result};

/// Contagens do outbox.
pub struct OutboxCounts {
    pub pending: i64,
    pub published: i64,
}

/// Pendentes/publicados no `uhura_outbox`.
pub async fn outbox_counts(client: &Client) -> Result<OutboxCounts> {
    let row = client
        .query_one(
            "SELECT \
               count(*) FILTER (WHERE published_at IS NULL)     AS pending, \
               count(*) FILTER (WHERE published_at IS NOT NULL) AS published \
             FROM uhura_outbox",
            &[],
        )
        .await
        .map_err(|e| Error::Storage(format!("outbox counts: {e}")))?;
    Ok(OutboxCounts {
        pending: row.get("pending"),
        published: row.get("published"),
    })
}

/// Total de registros no `uhura_inbox`.
pub async fn inbox_count(client: &Client) -> Result<i64> {
    let row = client
        .query_one("SELECT count(*) AS n FROM uhura_inbox", &[])
        .await
        .map_err(|e| Error::Storage(format!("inbox count: {e}")))?;
    Ok(row.get("n"))
}

/// Domínios distintos já vistos no outbox.
pub async fn distinct_domains(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT DISTINCT domain FROM uhura_outbox ORDER BY domain",
            &[],
        )
        .await
        .map_err(|e| Error::Storage(format!("distinct domains: {e}")))?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}
