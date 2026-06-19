//! Tipo de erro unificado do Uhura.

use thiserror::Error;

/// Erro de qualquer camada do bus.
#[derive(Debug, Error)]
pub enum Error {
    #[error("erro de configuração: {0}")]
    Config(String),

    #[error("erro de transporte: {0}")]
    Transport(String),

    #[error("erro de armazenamento: {0}")]
    Storage(String),

    #[error("erro de codec: {0}")]
    Codec(String),

    #[error("não implementado ainda: {0}")]
    Unimplemented(&'static str),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// `Result` padrão do Uhura.
pub type Result<T> = std::result::Result<T, Error>;
