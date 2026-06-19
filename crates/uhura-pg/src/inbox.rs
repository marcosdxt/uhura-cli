//! Acesso à tabela `uhura_inbox` (deduplicação / idempotência).

use tokio_postgres::Client;
use uhura_core::{Error, Result};

/// Operações sobre o inbox usando um client Postgres emprestado.
pub struct Inbox<'a> {
    client: &'a Client,
}

impl<'a> Inbox<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Tenta registrar o envelope como processado.
    ///
    /// Retorna `true` se é **novo** (deve processar) e `false` se **duplicado**
    /// (já processado → descartar). Em produção este `INSERT` roda na mesma
    /// transação do efeito de negócio (`SPEC.md` §12.1).
    pub async fn mark_processed(
        &self,
        envelope_id: &str,
        domain: &str,
        partitionkey: Option<&str>,
    ) -> Result<bool> {
        let n = self
            .client
            .execute(
                "INSERT INTO uhura_inbox (envelope_id, domain, partitionkey) \
                 VALUES ($1, $2, $3) ON CONFLICT (envelope_id) DO NOTHING",
                &[&envelope_id, &domain, &partitionkey],
            )
            .await
            .map_err(|e| Error::Storage(format!("insert no inbox: {e}")))?;
        Ok(n == 1)
    }
}
