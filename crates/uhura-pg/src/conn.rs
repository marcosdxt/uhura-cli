//! Conexão com o PostgreSQL.

use tokio_postgres::{Client, NoTls};
use uhura_core::{Error, Result};

/// Conecta ao Postgres e mantém a tarefa de conexão viva em background.
///
/// v1 em cluster privado usa `NoTls` (segurança não é foco — `SPEC.md` §15).
pub async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|e| Error::Storage(format!("conexão Postgres: {e}")))?;

    // A conexão precisa rodar numa tarefa própria enquanto o client é usado.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "conexão Postgres encerrada");
        }
    });

    Ok(client)
}
