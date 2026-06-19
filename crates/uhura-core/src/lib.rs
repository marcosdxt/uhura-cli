//! Tipos compartilhados do Uhura: envelope (CloudEvents), configuração e erros.
//!
//! Camada L0/ABI da especificação (ver `SPEC.md` §7). Mantida estável e versionada.

mod config;
mod envelope;
mod error;

pub use config::UhuraConfig;
pub use envelope::{Envelope, FactType, CLOUDEVENTS_SPEC_VERSION};
pub use error::{Error, Result};
