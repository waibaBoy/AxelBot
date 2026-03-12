# AxelBot

Risk-capped Rust market-making experiment scaffold for Polymarket-style prediction markets.

## What this implements

- Config-first bot architecture with six core modules:
  - `config`
  - `market_data`
  - `strategy`
  - `execution`
  - `risk`
  - `backtest`
- CLI commands:
  - `axelbot backtest --from ... --to ... --config ...`
  - `axelbot backtest-history --market ... --from ... --to ... --fidelity ... --config ...`
  - `axelbot api-check --config ... --market ...`
  - `axelbot dashboard --port 8080 --logs-dir logs`
  - `axelbot paper --config ...`
  - `axelbot paper-live --config ...` (real Polymarket WS data + simulated execution, with REST fallback if WS stalls)
  - `axelbot live --config ... --confirm-live`
- Risk controls:
  - Global drawdown stop (default `25%`)
  - Per-market exposure limit
  - Global aggregate exposure limit
  - Max open orders
  - Max order submission rate (orders/sec)
  - Stale-quote guard
  - Market data heartbeat timeout
  - Per-market circuit breaker on adverse slippage bursts
  - Manual emergency stop command (`STOP` on stdin)
- Metrics and structured JSONL logs with:
  - Realized/unrealized PnL
  - Inventory by market
  - Average order latency
  - Quote uptime ratio
  - Cancel ratio
  - Average slippage bps

## Quick start

1. Ensure Rust is installed (stable toolchain).
2. Configure `config.toml` (endpoints, wallet fields, risk/strategy params).
3. Optionally set secrets via env vars (recommended):
   - `AXELBOT_API_KEY`
   - `AXELBOT_API_SECRET`
   - `AXELBOT_WALLET_PRIVATE_KEY`
   - `AXELBOT_KILL_SWITCH`

The CLI auto-loads a local `.env` file on startup (via `dotenvy`).

### Backtest

```powershell
cargo run -- backtest --from 2026-01-01 --to 2026-01-03 --config config.toml
```

### Historical Replay Backtest (Polymarket price history)

```powershell
cargo run -- backtest-history --market <TOKEN_ID> --from 2026-01-01 --to 2026-01-03 --fidelity 1m --config config.toml
```

### API sanity check (server time + fee + tick size + optional history sample)

```powershell
cargo run -- api-check --config config.toml --market <TOKEN_ID>
```

### Fast local dashboard (real-time)

```powershell
cargo run -- dashboard --port 8080 --logs-dir logs
```

Then open:

```text
http://127.0.0.1:8080
```

## VS Code MCP (Polymarket)

This repo includes a workspace MCP config at:

```text
.vscode/mcp.json
```

It is configured for a public/read-only Polymarket MCP server command:

```text
polymarket-mcp
```

Install the server binary first (example):

```powershell
cargo install --git https://github.com/0x79de/polymarket-mcp
```

Then in VS Code:
1. Open this workspace.
2. Open Command Palette: `MCP: List Servers`.
3. Start `polymarket-public`.

## Get Official CLOB API Credentials (Rust, no Python)

Derive/create L2 credentials directly from this CLI:

```powershell
cargo run -- api-creds --config config.toml --rest-url https://clob.polymarket.com --nonce 0 --mode create-or-derive
```

Copy the printed lines into `.env`:
- `AXELBOT_API_KEY`
- `AXELBOT_API_SECRET`
- `AXELBOT_API_PASSPHRASE`

Legacy fallback helper (Python) is still available at:

```text
scripts/get_polymarket_api_creds.py
```

### Paper mode

```powershell
cargo run -- paper --config config.toml --ticks 3000
```

### Paper mode with live market data (no real orders)

```powershell
cargo run -- paper-live --config config.toml --ticks 3000
```

Type `STOP` + Enter at any time to trigger manual emergency stop.

### Live mode

```powershell
cargo run -- live --config config.toml --ticks 3000 --confirm-live
```

If live credentials are missing, the command safely falls back to simulated execution.

## Testing

```powershell
cargo test
```

Coverage includes:
- Risk checks (drawdown, exposure, order caps)
- Strategy quote math (spread/skew/inventory rebalance behavior)
- PnL accounting with fees
- Integration scenarios:
  - Partial fills
  - Out-of-order fills
  - Reconnect/replay duplicate fill handling
  - Cancel-race late fills
