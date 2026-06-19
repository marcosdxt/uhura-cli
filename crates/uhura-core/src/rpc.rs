//! Tipos de RPC sobre mensageria (ver `SPEC.md` §8.4).
//!
//! O formato de fio é compartilhado com o SDK NestJS: request JSON
//! `{id, domain, method, data}` e resposta `RpcResult`.

use serde::{Deserialize, Serialize};

/// Código de resultado de um método RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResCode {
    Ok,
    Error,
    Exception,
}

/// Envelope de resposta de um método RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResult<T> {
    pub data: Option<T>,
    #[serde(rename = "resCode")]
    pub res_code: ResCode,
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(rename = "errorStack", skip_serializing_if = "Option::is_none")]
    pub error_stack: Option<serde_json::Value>,
}

/// Requisição RPC enviada ao servidor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub id: String,
    pub domain: String,
    pub method: String,
    pub data: serde_json::Value,
}
