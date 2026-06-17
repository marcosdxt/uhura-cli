# `uhura` — CLI

```text
  _   _ _   _ _   _ ____      _
 | | | | | | | | | |  _ \    / \
 | | | | |_| | | | | |_) |  / _ \
 | |_| |  _  | |_| |  _ <  / ___ \
  \___/|_| |_|\___/|_| \_\/_/   \_\
   >> HAILING FREQUENCIES OPEN <<
```

**`uhura`** é a CLI de operação, diagnóstico e desenvolvimento do barramento
[Uhura](https://github.com/marcosdxt/uhura-nestjs) — escrita em **Rust**, binário único, que
**fala apenas o protocolo** (CloudEvents 1.0 + topologia AMQP do bus), sem nenhuma dependência
do pacote TypeScript.

> **Status: em especificação — pré-implementação.**
> A especificação canônica do ecossistema está em [`doc/uhura-spec.md`](./doc/uhura-spec.md)
> (cópia idêntica à do repo `uhura-bus`). A CLI é detalhada na **§19.6**; suas decisões de
> arquitetura, na **§22.11–12**. Nada abaixo está publicado ainda.

---

## Por que uma CLI própria (e não `rabbitmqadmin`/`kcat`)

As ferramentas genéricas do RabbitMQ não entendem as convenções do bus — envelope
CloudEvents, contratos versionados (`order.collected.v1`), headers `x-uhura-*` e DLQ com
diagnóstico. O `uhura` opera **no nível do barramento**, não do broker cru. Precedentes:
`nats` CLI, `kcat`.

Falando **somente os invariantes de wire** (spec §12), a CLI é também a **prova de
conformidade poliglota** do protocolo — o primeiro client não-TypeScript — e a incubadora do
futuro crate `uhura-bus-rs`.

## Princípios (normativos)

1. **Fala o protocolo, não a lib.** Implementa diretamente os invariantes de wire (§12); zero
   dependência do pacote TS.
2. **Observar nunca interfere.** `listen` cria fila exclusiva/autoDelete própria — jamais
   consome da fila de um grupo real. Tap é sempre seguro.
3. **Tudo que publica é atribuível.** `source = uhura-cli/<user>@<host>`, CloudEvent `id`
   próprio, header `x-uhura-protocol`.
4. **Scriptável.** Saída humana por default; `-o json` (NDJSON) para pipes; exit codes
   estáveis; sem prompt interativo quando há flag.
5. **Destrutivo é explícito.** `drain`, `dlq replay`, `dlq purge` exigem `--yes`.

## Comandos (v0 → v1)

```bash
# --- observação e publicação (v0) ---
uhura doctor                                          # pré-requisitos: conexão, permissões, plugin consistent-hash, versão
uhura contracts [--objeto order]                      # contratos/objetos conhecidos + bindings ativos no broker
uhura listen order.collected.v1 --status COLLECTED    # tap não-intrusivo (fila exclusiva própria)
uhura publish order collected --status COLLECTED --data @order.json
uhura send order cancel --data '{"id":"order-123","reason":"fraud"}'
uhura request order hydrate --data '{"id":"order-123"}' --timeout 10s
uhura topology show | uhura topology diff             # deriva a topologia canônica e diffa contra o broker
uhura queues [--service billing-service]              # profundidade, oldest age, consumidores, taxas (mgmt API)

# --- DLQ, docs e frota (v1) ---
uhura dlq ls billing-service.billing.dead             # inspeção: payload + headers de diagnóstico
uhura dlq replay billing-service.billing.dead --yes   # devolve à fila original (republish correto)
uhura dlq purge <fila> --yes  |  uhura drain <fila> --yes
uhura docs --out BUS.md [--live]                       # catálogo da frota a partir dos manifestos
uhura contracts init [--dir]                           # scaffold do repo de contratos do mesh
uhura bump [--config uhura.fleet.json] [--check|--pr|--push]   # sincroniza a frota com o repo de contratos
```

**Configuração:** `RABBITMQ_URL` + `UHURA_MGMT_URL` (ou flags), `--vhost`, `--prefix`
(= `exchangePrefix`), `--contracts <dir>`.

## Arquitetura

Workspace Cargo com dois crates (decisões §22.11–12):

| Crate | Papel |
|---|---|
| `uhura-core` | envelope CloudEvents, derivação de topologia, publisher confirm, direct reply-to — o embrião do `uhura-bus-rs` (spec §12) |
| `uhura-cli` | camada de comando (`clap`) sobre o core |

Stack: `clap` · `lapin` (AMQP) · `tokio` · `serde`/`serde_json` (structs próprias de envelope) ·
`reqwest` (management API). Distribuição: **binário único por release** (linux x86_64/aarch64),
via GitHub Releases — máquinas de operação não precisam de toolchain Node.

## Qualidade

Mesma política dos demais projetos (spec §14.5): unit (`cargo test`) + integração
(`testcontainers-rs` contra `rabbitmq:4-management`), **round-trip de interop TS↔Rust** contra
os golden files de conformidade do repo `uhura-bus`, cobertura (`cargo-llvm-cov`) com diff
coverage 100% por PR e mutation testing (`cargo-mutants`) nos módulos core.

## Roadmap

- [ ] **Spike S8** (spec §26.1): esqueleto Rust — `lapin` publica com confirm + consome tap +
      direct reply-to, interoperando com mensagem publicada pelo lado TS.
- [ ] **CLI v0** (em paralelo à Fase 2 do bus): `listen`, `publish`, `send`, `request`,
      `topology diff`, `queues`, `doctor`.
- [ ] **CLI v1** (antes do piloto, Fase 3): `dlq ls/show/replay`, `docs`, `contracts init`, `bump`.
- [ ] **Pós-0.2:** promoção do `uhura-core` ao crate publicado `uhura-bus-rs` quando houver o
      primeiro serviço Rust.

## 📖 Documentação

A [Especificação (doc/uhura-spec.md)](./doc/uhura-spec.md) é o documento canônico do
ecossistema — a CLI vive na §19.6 e nas decisões §22.11–12.

---
*Developed with focus on reliability, scalability, and elegance.*
