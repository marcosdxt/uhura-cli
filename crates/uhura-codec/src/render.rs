//! Geração de código Rust e documentação a partir dos contratos.

use std::fmt::Write;

use crate::parse::{ts_to_rust, Contract};

/// Gera um módulo Rust com os structs dos contratos.
pub fn render_rust(contracts: &[Contract]) -> String {
    let mut out = String::new();
    out.push_str("// Gerado por `uhura sync` — não edite à mão.\n");
    out.push_str("#![allow(non_snake_case, dead_code)]\n\n");
    out.push_str("use serde::{Deserialize, Serialize};\n\n");

    for c in contracts {
        let events = c.events.join(", ");
        let _ = writeln!(out, "/// Contrato `{}` (eventos: {}).", c.domain, events);
        if let Some(pid) = &c.partition_id {
            let _ = writeln!(out, "/// Partição: `{pid}`.");
        }
        out.push_str("#[derive(Debug, Clone, Serialize, Deserialize)]\n");
        let _ = writeln!(out, "pub struct {} {{", c.name);
        for f in &c.fields {
            let rust = ts_to_rust(&f.ts_type);
            if f.optional {
                out.push_str("    #[serde(skip_serializing_if = \"Option::is_none\")]\n");
                let _ = writeln!(out, "    pub {}: Option<{}>,", f.name, rust);
            } else {
                let _ = writeln!(out, "    pub {}: {},", f.name, rust);
            }
        }
        out.push_str("}\n\n");
    }
    out
}

/// Gera documentação Markdown (para LLM e navegação).
pub fn render_md(contracts: &[Contract]) -> String {
    let mut out = String::new();
    out.push_str("# Contratos Uhura\n\n");
    out.push_str("> Gerado por `uhura sync`.\n\n");
    for c in contracts {
        let _ = writeln!(out, "## `{}` — {}\n", c.domain, c.name);
        let _ = writeln!(out, "- **Eventos:** {}", join_or(&c.events, "—"));
        if let Some(pid) = &c.partition_id {
            let _ = writeln!(out, "- **Partição:** `{pid}`");
        }
        out.push_str("\n| Campo | Tipo | Opcional |\n|-------|------|----------|\n");
        for f in &c.fields {
            let _ = writeln!(
                out,
                "| `{}` | `{}` | {} |",
                f.name,
                f.ts_type,
                if f.optional { "sim" } else { "não" }
            );
        }
        out.push('\n');
    }
    out
}

/// Gera uma página HTML pesquisável simples.
pub fn render_html(contracts: &[Contract]) -> String {
    let mut body = String::new();
    for c in contracts {
        let _ = write!(
            body,
            "<section><h2>{} <small>{}</small></h2><p>Eventos: {}</p><table>\
             <tr><th>Campo</th><th>Tipo</th><th>Opcional</th></tr>",
            html_escape(&c.domain),
            html_escape(&c.name),
            html_escape(&join_or(&c.events, "—"))
        );
        for f in &c.fields {
            let _ = write!(
                body,
                "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                html_escape(&f.name),
                html_escape(&f.ts_type),
                if f.optional { "sim" } else { "não" }
            );
        }
        body.push_str("</table></section>");
    }
    format!(
        "<!doctype html><html lang=\"pt-BR\"><head><meta charset=\"utf-8\">\
         <title>Contratos Uhura</title><style>body{{font-family:sans-serif;margin:2rem}}\
         table{{border-collapse:collapse}}td,th{{border:1px solid #ccc;padding:4px 8px}}\
         section{{margin-bottom:2rem}}</style></head><body>\
         <h1>Contratos Uhura</h1>{body}</body></html>"
    )
}

fn join_or(items: &[String], empty: &str) -> String {
    if items.is_empty() {
        empty.to_string()
    } else {
        items.join(", ")
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
