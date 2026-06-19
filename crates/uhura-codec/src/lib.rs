//! Codegen de contratos e documentação (`uhura sync` / `uhura doc`).
//!
//! Lê o repositório de contratos (`.ts` com `@UhuraContract`) e gera structs
//! Rust + documentação (ver `SPEC.md` §6).

mod parse;
mod render;

use std::fs;
use std::path::{Path, PathBuf};

use uhura_core::{Error, Result};

use crate::parse::Contract;

/// Gera structs Rust e documentação a partir do repositório de contratos.
pub fn sync(contracts_dir: &str, out_dir: &str) -> Result<()> {
    let contracts = parse_dir(contracts_dir)?;
    if contracts.is_empty() {
        tracing::warn!(dir = %contracts_dir, "nenhum contrato (@UhuraContract) encontrado");
        return Ok(());
    }
    fs::create_dir_all(out_dir).map_err(|e| Error::Codec(format!("criar {out_dir}: {e}")))?;

    write_file(out_dir, "contracts.rs", &render::render_rust(&contracts))?;
    write_file(out_dir, "CONTRACTS.md", &render::render_md(&contracts))?;

    tracing::info!(
        count = contracts.len(),
        out = %out_dir,
        "contratos gerados (contracts.rs, CONTRACTS.md)"
    );
    Ok(())
}

/// Gera apenas a documentação (Markdown + HTML).
pub fn generate_docs(contracts_dir: &str, out_dir: &str) -> Result<()> {
    let contracts = parse_dir(contracts_dir)?;
    if contracts.is_empty() {
        tracing::warn!(dir = %contracts_dir, "nenhum contrato encontrado");
        return Ok(());
    }
    fs::create_dir_all(out_dir).map_err(|e| Error::Codec(format!("criar {out_dir}: {e}")))?;

    write_file(out_dir, "CONTRACTS.md", &render::render_md(&contracts))?;
    write_file(out_dir, "index.html", &render::render_html(&contracts))?;

    tracing::info!(count = contracts.len(), out = %out_dir, "documentação gerada");
    Ok(())
}

fn parse_dir(dir: &str) -> Result<Vec<Contract>> {
    let mut files = Vec::new();
    collect_ts(Path::new(dir), &mut files)?;
    let mut contracts = Vec::new();
    for path in files {
        let src =
            fs::read_to_string(&path).map_err(|e| Error::Codec(format!("ler {path:?}: {e}")))?;
        contracts.extend(parse::parse_source(&src)?);
    }
    contracts.sort_by(|a, b| a.domain.cmp(&b.domain));
    Ok(contracts)
}

fn collect_ts(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries =
        fs::read_dir(dir).map_err(|e| Error::Codec(format!("ler diretório {dir:?}: {e}")))?;
    for entry in entries {
        let path = entry
            .map_err(|e| Error::Codec(format!("entrada: {e}")))?
            .path();
        if path.is_dir() {
            collect_ts(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("ts")
            && !path.to_string_lossy().ends_with(".d.ts")
        {
            out.push(path);
        }
    }
    Ok(())
}

fn write_file(dir: &str, name: &str, content: &str) -> Result<()> {
    let path = Path::new(dir).join(name);
    fs::write(&path, content).map_err(|e| Error::Codec(format!("escrever {path:?}: {e}")))
}
