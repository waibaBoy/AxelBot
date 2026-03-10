use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{config::StrategyConfig, market_data::MarketSnapshot};

#[derive(Debug, Clone, Serialize)]
pub struct QuoteProposal {
    pub market: String,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub generated_at: DateTime<Utc>,
}

pub struct MarketMakingStrategy {
    cfg: StrategyConfig,
}

impl MarketMakingStrategy {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self { cfg }
    }

    pub fn refresh_interval_ms(&self) -> i64 {
        self.cfg.refresh_interval_ms
    }

    pub fn generate_quote(&self, snapshot: &MarketSnapshot, inventory: f64) -> QuoteProposal {
        let spread = (self.cfg.target_spread_bps / 10_000.0) * snapshot.fair_value;
        let skew = (self.cfg.inventory_skew_bps / 10_000.0) * snapshot.fair_value * inventory;
        let centered_fair = snapshot.fair_value - skew;

        let mut bid = centered_fair - spread * 0.5;
        let mut ask = centered_fair + spread * 0.5;
        if ask <= bid {
            ask = bid + 0.0001;
        }
        bid = bid.clamp(0.01, 0.99);
        ask = ask.clamp(0.01, 0.99);

        let base_size = self.cfg.quote_sizes[0];
        let size_bias = (inventory.abs() / 50.0).min(0.8);
        let (bid_size, ask_size) = if inventory > 0.0 {
            (base_size * (1.0 - size_bias), base_size * (1.0 + size_bias))
        } else if inventory < 0.0 {
            (base_size * (1.0 + size_bias), base_size * (1.0 - size_bias))
        } else {
            (base_size, base_size)
        };

        QuoteProposal {
            market: snapshot.market.clone(),
            bid_price: bid,
            ask_price: ask,
            bid_size: bid_size.max(0.01),
            ask_size: ask_size.max(0.01),
            generated_at: snapshot.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn cfg() -> StrategyConfig {
        StrategyConfig {
            target_spread_bps: 30.0,
            inventory_skew_bps: 12.0,
            quote_sizes: vec![5.0],
            refresh_interval_ms: 1_000,
        }
    }

    fn snapshot() -> MarketSnapshot {
        MarketSnapshot {
            market: "TEST".to_string(),
            bid: 0.49,
            ask: 0.51,
            mid: 0.5,
            fair_value: 0.5,
            spread_bps: 400.0,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn generates_positive_spread() {
        let strat = MarketMakingStrategy::new(cfg());
        let quote = strat.generate_quote(&snapshot(), 0.0);
        assert!(quote.ask_price > quote.bid_price);
    }

    #[test]
    fn long_inventory_skews_down() {
        let strat = MarketMakingStrategy::new(cfg());
        let neutral = strat.generate_quote(&snapshot(), 0.0);
        let long = strat.generate_quote(&snapshot(), 20.0);
        assert!(long.bid_price < neutral.bid_price);
        assert!(long.ask_size > neutral.ask_size);
    }

    #[test]
    fn short_inventory_skews_up() {
        let strat = MarketMakingStrategy::new(cfg());
        let neutral = strat.generate_quote(&snapshot(), 0.0);
        let short = strat.generate_quote(&snapshot(), -20.0);
        assert!(short.ask_price > neutral.ask_price);
        assert!(short.bid_size > neutral.bid_size);
    }
}
