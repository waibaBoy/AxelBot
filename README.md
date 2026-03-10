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
  - `axelbot paper --config ...`
  - `axelbot live --config ... --confirm-live`
- Risk controls:
  - Global drawdown stop (default `25%`)
  - Per-market exposure limit
  - Max open orders
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

### Backtest

```powershell
cargo run -- backtest --from 2026-01-01 --to 2026-01-03 --config config.toml
```

### Paper mode

```powershell
cargo run -- paper --config config.toml --ticks 3000
```

Type `STOP` + Enter at any time to trigger manual emergency stop.

### Live mode (simulated adapter scaffold)

```powershell
cargo run -- live --config config.toml --ticks 3000 --confirm-live
```

This project currently uses a simulated exchange adapter; live adapter wiring should replace `SimulatedExchangeClient` in `execution`.

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
