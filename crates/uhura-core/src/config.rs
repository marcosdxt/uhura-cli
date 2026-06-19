//! Configuração de runtime do bus.

/// Configuração mínima compartilhada por SDKs e engine.
#[derive(Debug, Clone)]
pub struct UhuraConfig {
    /// URL AMQP do RabbitMQ (`amqp://` em cluster privado na v1).
    pub amqp_url: String,
    /// URL do PostgreSQL — fonte de verdade (outbox/inbox).
    pub postgres_url: String,
    /// Nome do mesh (prefixo de domínio: `uhura.<mesh>.`).
    pub mesh: String,
    /// Logging detalhado e tracing granular.
    pub debug: bool,
}
