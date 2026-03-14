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
- Inventory protection in execution:
  - Inventory-aware quote size damping near per-market risk limit
  - Hard-side flattening when inventory approaches configured hard limit
  - Pre-trade per-side size clipping to exposure headroom (reduces avoidable exposure-limit rejects)
- Fair value and alpha inputs:
  - Micro-price enhanced fair value in market data cache
  - Short-horizon predictive order-flow signal (`order_flow_signal`, `alpha_bps`) used to skew quotes
  - Optional news sentiment overlay (Guardian) applied as quote bias
- Metrics and structured JSONL logs with:
  - Realized/unrealized PnL
  - Inventory by market
  - Average order latency
  - Average fill latency
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
   - `AXELBOT_GUARDIAN_API_KEY`
   - `AXELBOT_PROXY_URL` (optional, explicit proxy only)
   - `AXELBOT_NEWS_ENABLED`
   - `AXELBOT_KILL_SWITCH`

The CLI auto-loads a local `.env` file on startup (via `dotenvy`).

Proxy behavior:
- AxelBot uses a proxy only when explicitly configured in `config.toml` (`exchange.proxy_url`) or `AXELBOT_PROXY_URL`.
- Ambient system proxy vars (`HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY`) are ignored by default to keep startup deterministic.
- For SOCKS5, verify the endpoint is reachable first (example: `Test-NetConnection 127.0.0.1 -Port 1080`).

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

Paper-live simulation realism is configurable in `[execution]`:
- `sim_fill_min_latency_ms` / `sim_fill_max_latency_ms`
- `sim_slippage_bps`
- `inventory_soft_limit_ratio` / `inventory_hard_limit_ratio`
- `inventory_min_size_scale` / `inventory_flatten_boost`

### Paper tuning profile (2026-03-13 A)

Current default paper-tuning profile in `config.toml`:
- `risk.max_per_market_exposure = 8.0`
- `execution.inventory_soft_limit_ratio = 0.30`
- `execution.inventory_hard_limit_ratio = 0.50`
- `execution.inventory_flatten_boost = 2.50`
- `strategy.inventory_skew_bps = 24.0`
- `strategy.target_spread_bps = 34.0`
- `strategy.quote_sizes = [0.7, 1.4, 2.1]`

Reference command:

```powershell
cargo run -- paper-live --config config.toml --ticks 300
```

Reference run output path to track tuning progression:
- `logs\paper-live-20260313T015246.jsonl`

### Paper tuning profile (2026-03-13 B)

Profile used for the previous batch:
- `risk.max_per_market_exposure = 6.0`
- `execution.inventory_soft_limit_ratio = 0.25`
- `execution.inventory_hard_limit_ratio = 0.40`
- `execution.inventory_flatten_boost = 2.50`
- `strategy.inventory_skew_bps = 30.0`
- `strategy.target_spread_bps = 38.0`
- `strategy.quote_sizes = [0.5, 1.0, 1.5]`

Evaluation command:

```powershell
1..5 | ForEach-Object { cargo run -- paper-live --config config.toml --ticks 300 }
```

### Paper tuning profile (2026-03-13 C, current)

Current active profile in `config.toml`:
- `risk.max_per_market_exposure = 4.5`
- `execution.inventory_soft_limit_ratio = 0.20`
- `execution.inventory_hard_limit_ratio = 0.33`
- `execution.inventory_flatten_boost = 3.00`
- `strategy.inventory_skew_bps = 38.0`
- `strategy.target_spread_bps = 46.0`
- `strategy.quote_sizes = [0.35, 0.7, 1.05]`

Evaluation command:

```powershell
1..5 | ForEach-Object { cargo run -- paper-live --config config.toml --ticks 300 }
```

Type `STOP` + Enter at any time to trigger manual emergency stop.

## Offline model training (Colab-ready)

AxelBot now logs `feature_sample` events during quote generation. Use these to train
an offline short-horizon direction model.

1. Generate paper-live logs:

```powershell
1..20 | ForEach-Object { cargo run -- paper-live --config config.toml --ticks 300 }
```

2. Export labeled CSV dataset:

```powershell
python scripts/export_feature_dataset.py --input "logs/paper-live-*.jsonl" --output "data/feature_dataset.csv" --horizon-events 3 --threshold-bps 2.0 --max-book-spread 0.50
```

`--max-book-spread` removes empty-book/sentinel transitions (for example `0.001/0.999`) from label generation.
At runtime, AxelBot rejects extreme book updates (`> 0.90` absolute spread) before they enter `feature_sample` logs.

3. Upload `data/feature_dataset.csv` to Colab and train a baseline model:
- Start with logistic regression or gradient boosting.
- Features: `micro_price`, `fair_value`, `spread_bps`, `imbalance`, `order_flow_signal`, `alpha_bps`, `inventory`, quote/market top-of-book fields.
- Target: `label` (`-1`, `0`, `1`; neutral optional).

4. Validate out-of-sample and compare MTM against the current rule-based baseline before enabling live signal usage.

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
