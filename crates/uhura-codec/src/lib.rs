//! Codegen de contratos e documentação (scaffold).
//!
//! Lê o repositório de contratos (submódulo) e gera tipos NestJS/Rust + docs
//! (ver `SPEC.md` §6).

use uhura_core::{Error, Result};

/// Gera tipos NestJS/Rust e roda compat-check a partir do repositório de contratos.
pub fn sync(contracts_dir: &str, out_dir: &str) -> Result<()> {
    tracing::debug!(target: "uhura_codec", contracts = %contracts_dir, out = %out_dir, "sync (scaffold)");
    Err(Error::Unimplemented("sync: codegen de contratos"))
}

/// Gera documentação (HTML pesquisável + `.md` para LLM).
pub fn generate_docs(contracts_dir: &str, out_dir: &str) -> Result<()> {
    tracing::debug!(target: "uhura_codec", contracts = %contracts_dir, out = %out_dir, "doc (scaffold)");
    Err(Error::Unimplemented("doc: geração de documentação"))
}
