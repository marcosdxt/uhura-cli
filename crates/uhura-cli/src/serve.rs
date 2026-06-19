//! Backend HTTP de métricas para o `uhura-console`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio_postgres::Client;
use tower_http::cors::CorsLayer;
use uhura_transport::rabbitmq::RabbitMqTransport;

#[derive(Clone)]
struct AppState {
    pg: Arc<Client>,
    transport: Arc<RabbitMqTransport>,
}

#[derive(Serialize)]
struct Overview {
    outbox: OutboxDto,
    inbox: InboxDto,
    domains: Vec<DomainDto>,
}

#[derive(Serialize)]
struct OutboxDto {
    pending: i64,
    published: i64,
}

#[derive(Serialize)]
struct InboxDto {
    total: i64,
}

#[derive(Serialize)]
struct DomainDto {
    domain: String,
    main: u32,
    parking: u32,
}

/// Sobe o servidor de métricas em `0.0.0.0:<port>`.
pub async fn run(
    port: u16,
    pg: Arc<Client>,
    transport: Arc<RabbitMqTransport>,
) -> anyhow::Result<()> {
    let state = AppState { pg, transport };
    let app = Router::new()
        .route("/api/health", get(|| async { "ok" }))
        .route("/api/overview", get(overview))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "uhura serve (backend de métricas) ativo");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn overview(State(state): State<AppState>) -> Result<Json<Overview>, ApiError> {
    let counts = uhura_pg::stats::outbox_counts(&state.pg).await?;
    let inbox = uhura_pg::stats::inbox_count(&state.pg).await?;
    let domains = uhura_pg::stats::distinct_domains(&state.pg).await?;

    let mut dtos = Vec::with_capacity(domains.len());
    for domain in domains {
        let (main, parking) = state.transport.depths(&domain).await.unwrap_or((0, 0));
        dtos.push(DomainDto {
            domain,
            main,
            parking,
        });
    }

    Ok(Json(Overview {
        outbox: OutboxDto {
            pending: counts.pending,
            published: counts.published,
        },
        inbox: InboxDto { total: inbox },
        domains: dtos,
    }))
}

/// Erro de API mapeado para HTTP 500.
struct ApiError(String);

impl From<uhura_core::Error> for ApiError {
    fn from(e: uhura_core::Error) -> Self {
        ApiError(e.to_string())
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0).into_response()
    }
}
