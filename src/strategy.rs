use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{config::StrategyConfig, market_data::MarketSnapshot};

#[derive(Debug, Clone, Serialize)]
pub struct QuoteProposal {
    pub market: String,
    pub market_bid: f64,
    pub market_ask: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub generated_at: DateTime<Utc>,
}

pub struct MarketMakingStrategy {
    cfg: StrategyConfig,
    state: HashMap<String, StrategyState>,
    news_bias_bps: HashMap<String, f64>,
}

#[derive(Debug, Clone, Default)]
struct StrategyState {
    last_mid: f64,
    ewma_abs_return_bps: f64,
}

impl MarketMakingStrategy {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self {
            cfg,
            state: HashMap::new(),
            news_bias_bps: HashMap::new(),
        }
    }

    pub fn refresh_interval_ms(&self) -> i64 {
        self.cfg.refresh_interval_ms
    }

    pub fn set_news_bias_bps(&mut self, market: &str, bias_bps: f64) {
        self.news_bias_bps
            .insert(market.to_string(), bias_bps.clamp(-100.0, 100.0));
    }

    pub fn generate_quote(&mut self, snapshot: &MarketSnapshot, inventory: f64) -> QuoteProposal {
        let state = self.state.entry(snapshot.market.clone()).or_default();
        if state.last_mid > 0.0 && snapshot.mid > 0.0 {
            let abs_ret_bps = ((snapshot.mid / state.last_mid) - 1.0).abs() * 10_000.0;
            state.ewma_abs_return_bps = 0.90 * state.ewma_abs_return_bps + 0.10 * abs_ret_bps;
        }
        state.last_mid = snapshot.mid;

        let base_spread_bps = self.cfg.target_spread_bps.max(1.0);
        let vol_mult = (1.0 + (state.ewma_abs_return_bps / 8.0)).clamp(1.0, 3.5);
        let market_spread_mult = (snapshot.spread_bps / base_spread_bps).clamp(0.7, 3.0);
        let adaptive_spread_bps = base_spread_bps * vol_mult * market_spread_mult.sqrt();
        let spread = (adaptive_spread_bps / 10_000.0) * snapshot.fair_value;

        let inv_skew = (self.cfg.inventory_skew_bps / 10_000.0) * snapshot.fair_value * inventory;
        // Top-of-book depth imbalance: positive means bid side is heavier than ask side.
        let depth_skew = (self.cfg.inventory_skew_bps / 10_000.0)
            * snapshot.fair_value
            * snapshot.imbalance
            * 0.5;
        let news_shift = self
            .news_bias_bps
            .get(&snapshot.market)
            .copied()
            .unwrap_or(0.0)
            / 10_000.0
            * snapshot.fair_value;
        let centered_fair = snapshot.fair_value - inv_skew - depth_skew + news_shift;

        let mut bid = centered_fair - spread * 0.5;
        let mut ask = centered_fair + spread * 0.5;
        if ask <= bid {
            ask = bid + 0.0001;
        }
        bid = bid.clamp(0.01, 0.99);
        ask = ask.clamp(0.01, 0.99);

        let depth = (snapshot.bid_size + snapshot.ask_size).max(0.0);
        let depth_scale = (depth / 20.0).clamp(0.25, 2.0);
        let vol_size_scale = (1.0 / (1.0 + state.ewma_abs_return_bps / 25.0)).clamp(0.35, 1.0);
        let base_size = self.cfg.quote_sizes[0] * depth_scale * vol_size_scale;
        let size_bias = (inventory.abs() / 50.0).min(0.8);
        let (mut bid_size, mut ask_size) = if inventory > 0.0 {
            (base_size * (1.0 - size_bias), base_size * (1.0 + size_bias))
        } else if inventory < 0.0 {
            (base_size * (1.0 + size_bias), base_size * (1.0 - size_bias))
        } else {
            (base_size, base_size)
        };
        let imbalance_bias = snapshot.imbalance * 0.25;
        bid_size *= (1.0 - imbalance_bias).clamp(0.5, 1.5);
        ask_size *= (1.0 + imbalance_bias).clamp(0.5, 1.5);

        QuoteProposal {
            market: snapshot.market.clone(),
            market_bid: snapshot.bid,
            market_ask: snapshot.ask,
            bid_price: bid,
            ask_price: ask,
            bid_size: bid_size.max(0.01),
            ask_size: ask_size.max(0.01),
            generated_at: Utc::now(),
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
            bid_size: 10.0,
            ask: 0.51,
            ask_size: 12.0,
            mid: 0.5,
            fair_value: 0.5,
            spread_bps: 400.0,
            imbalance: -0.09,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn generates_positive_spread() {
        let mut strat = MarketMakingStrategy::new(cfg());
        let quote = strat.generate_quote(&snapshot(), 0.0);
        assert!(quote.ask_price > quote.bid_price);
    }

    #[test]
    fn long_inventory_skews_down() {
        let mut strat = MarketMakingStrategy::new(cfg());
        let neutral = strat.generate_quote(&snapshot(), 0.0);
        let long = strat.generate_quote(&snapshot(), 20.0);
        assert!(long.bid_price < neutral.bid_price);
        assert!(long.ask_size > neutral.ask_size);
    }

    #[test]
    fn short_inventory_skews_up() {
        let mut strat = MarketMakingStrategy::new(cfg());
        let neutral = strat.generate_quote(&snapshot(), 0.0);
        let short = strat.generate_quote(&snapshot(), -20.0);
        assert!(short.ask_price > neutral.ask_price);
        assert!(short.bid_size > neutral.bid_size);
    }
}
