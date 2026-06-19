//! Acesso à tabela `uhura_outbox` (escrita pelo produtor, lida pelo dispatcher).

use tokio_postgres::Client;
use uhura_core::{Envelope, Error, Result};

/// Uma linha do outbox, com o id necessário para marcar publicação.
pub struct OutboxRecord {
    pub id: i64,
    pub domain: String,
    pub event: String,
    pub partitionkey: Option<String>,
    pub envelope: Envelope,
}

/// Operações sobre o outbox usando um client Postgres emprestado.
pub struct Outbox<'a> {
    client: &'a Client,
}

impl<'a> Outbox<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Insere um evento no outbox e devolve o id gerado.
    ///
    /// Em produção isto roda na MESMA transação do write de negócio; aqui é
    /// usado pela injeção `uhura publish`.
    pub async fn insert(
        &self,
        domain: &str,
        event: &str,
        partitionkey: Option<&str>,
        envelope: &Envelope,
    ) -> Result<i64> {
        let json = serde_json::to_value(envelope)
            .map_err(|e| Error::Storage(format!("serialização do envelope: {e}")))?;
        let row = self
            .client
            .query_one(
                "INSERT INTO uhura_outbox (domain, event, partitionkey, envelope) \
                 VALUES ($1, $2, $3, $4) RETURNING id",
                &[&domain, &event, &partitionkey, &json],
            )
            .await
            .map_err(|e| Error::Storage(format!("insert no outbox: {e}")))?;
        Ok(row.get(0))
    }

    /// Lê o próximo lote de eventos não publicados, em ordem de id (commit).
    pub async fn fetch_unpublished(&self, limit: i64) -> Result<Vec<OutboxRecord>> {
        let rows = self
            .client
            .query(
                "SELECT id, domain, event, partitionkey, envelope \
                 FROM uhura_outbox WHERE published_at IS NULL ORDER BY id LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|e| Error::Storage(format!("select no outbox: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let json: serde_json::Value = row.get("envelope");
            let envelope: Envelope = serde_json::from_value(json)
                .map_err(|e| Error::Storage(format!("desserialização do envelope: {e}")))?;
            out.push(OutboxRecord {
                id: row.get("id"),
                domain: row.get("domain"),
                event: row.get("event"),
                partitionkey: row.get("partitionkey"),
                envelope,
            });
        }
        Ok(out)
    }

    /// Marca um conjunto de ids como publicados (após confirmação do broker).
    pub async fn mark_published(&self, ids: &[i64]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let n = self
            .client
            .execute(
                "UPDATE uhura_outbox SET published_at = now() WHERE id = ANY($1)",
                &[&ids],
            )
            .await
            .map_err(|e| Error::Storage(format!("marcar publicados: {e}")))?;
        Ok(n)
    }
}
