# uhura-cli

CLI e *engine* do **Uhura** — message bus para mesh de microserviços, *contract-first*, sobre RabbitMQ + PostgreSQL.

> Binário: `uhura` (Rust). Parte do projeto Uhura. Especificação formal completa em [`dextro-message-bus/SPEC.md`](../dextro-message-bus/SPEC.md).

## O que este repositório entrega

Dois papéis no mesmo binário:

1. **CLI `uhura`** — todas as operações do bus (codegen, schema do banco, topologia, monitoração, parking/replay, injeção de primitivas, docs).
2. **`uhura-station`** (`uhura station`) — *engine* de instrumentação: o **único componente com estado operacional ativo**. Lê o WAL do Postgres (logical decoding) / o *outbox*, publica no RabbitMQ com *publisher confirms*, serve RPC de administração e expõe métricas para o `uhura-console`.

## Comandos

| Comando | Função |
|---------|--------|
| `uhura sync` | Codegen (tipos NestJS/Rust), docs (HTML + `.md`), *compat-check* de contratos. |
| `uhura db init` / `uhura db sync` | `wal_level`/*slot*, tabelas `uhura_outbox`/`uhura_inbox`, triggers (modo compat) e *migrations* a partir de `.cdc`. |
| `uhura station` | Sobe o *engine* (WAL reader + dispatcher + RPC admin + backend de métricas). |
| `uhura topology apply` | Cria/valida exchanges, quorum queues, DLX, parking, *alternate-exchange*, bindings *consistent-hash*. |
| `uhura top` | TUI de monitoração (filas, lag de slot, mensagens paradas, taxa). |
| `uhura parking list` / `uhura parking replay` | Lista e **reenvia** mensagens do parking lot (libera quarentena de partição). |
| `uhura publish` / `uhura method` | Injeção de primitivas (publicar evento / chamar RPC). |
| `uhura doc` | Gera/serve a documentação de contratos e serviços. |

## Engine (`uhura-station`)

- **CDC preferencial**: WAL logical decoding (`pgoutput`), cursor durável em `confirmed_flush_lsn`, ordem de commit. Baseline **PostgreSQL ≥ 16** + `pg_failover_slots` (sem exigir PG 17). **Um consumidor por slot**.
- **Modo compatível**: trigger + *outbox polling* (acordado por `LISTEN/NOTIFY`).
- **Alta disponibilidade**: ativo/standby com *leader-election* (Lease k8s / advisory-lock), um líder por *slot*/shard; *sharding* por domínio para escalar a leitura do WAL.
- **Reliability**: *publisher confirms*, *backpressure* via `reject-publish` (segura no WAL/outbox, sem perda), *lease/visibility-timeout* para reclamar `processing` órfão.

## Status

🚧 Em implementação a partir da `SPEC.md`. Recomendação de sequenciamento (§23.3 da spec): MVP em trigger/polling → trocar por WAL mantendo a ABI → endurecer operação.

## Layout do workspace

Workspace Cargo modular, alinhado às camadas da spec:

```
crates/
  uhura-core/       # L0: envelope CloudEvents, config, erros (ABI estável)
  uhura-transport/  # L1: trait UhuraTransport + driver RabbitMQ
  uhura-pg/         # L2: outbox/inbox, schema, reader WAL/polling
  uhura-engine/     # uhura-station: dispatcher + WAL reader + leader-election
  uhura-codec/      # codegen de contratos (sync) + docs
  uhura-cli/        # binário `uhura` (clap): roteia os subcomandos
```

Estado atual: **MVP de entrega funcional** (verificado end-to-end com Postgres +
RabbitMQ reais):

- `uhura db init` — cria `uhura_outbox`/`uhura_inbox` + trigger de NOTIFY.
- `uhura publish <domínio> <evento> --data <json> [--partition k]` — grava o
  evento (envelope CloudEvents) no outbox.
- `uhura topology apply --domain <d>` — declara exchange + quorum queue + DLX/parking.
- `uhura station` — lê o outbox em ordem, publica com **publisher confirms**,
  marca como publicado só após `ack`; para no 1º erro do lote (ordem/backpressure).
- `uhura consume <domínio> [--reject k]` — consome com **idempotência** (Inbox),
  `ack` no sucesso, `nack`→retry/parking no poison.
- `uhura parking replay --domain <d>` — reenvia o parking para a exchange.
- `uhura top --domain <d>` — contagem das filas main/parking.
- `uhura call <domínio> <método> --data <json>` — cliente RPC (request/reply
  via direct reply-to + correlationId); imprime o `RpcResult`.

Loop de confiabilidade **verificado end-to-end** (publish → station → consume →
dedup → poison → parking → replay) com Postgres + RabbitMQ reais, e coberto por
testes de integração (testcontainers) em `uhura-pg` e `uhura-transport`.

Cliente RPC (`uhura call`) **verificado em interop** contra um servidor
`@UhuraFunction` do SDK NestJS.

Ainda `Unimplemented`: `sync`/`doc` (codegen de contratos), `db sync` (.cdc),
e o WAL logical decoding (entra sem mudar a ABI). (`method` é alias legado de
`call`.)

## Desenvolvimento

```bash
cargo build --all-targets
cargo test --all                                   # unitários
cargo test -p uhura-pg --features integration      # integração (Docker/testcontainers)
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Exemplo end-to-end local

```bash
export UHURA_PG_URL="postgres://uhura:uhura@127.0.0.1:5544/uhura"
export UHURA_AMQP_URL="amqp://guest:guest@127.0.0.1:5673"
uhura db init
uhura publish usuario.info started --data '{"id":"42"}' --partition 42
uhura station          # despacha; Ctrl-C para sair
```
