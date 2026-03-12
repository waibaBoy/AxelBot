use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn sign(self) -> f64 {
        match self {
            Side::Bid => 1.0,
            Side::Ask => -1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Yes,
    No,
}

/// Maps a binary market to its two token IDs on Polymarket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketPair {
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub neg_risk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub market: String,
    /// Polymarket CLOB token ID (set when trading live).
    #[serde(default)]
    pub token_id: Option<String>,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub quote_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvent {
    pub fill_id: String,
    pub order_id: String,
    pub market: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub expected_price: f64,
    pub fee_paid: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Position {
    pub qty: f64,
    pub avg_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub cash: f64,
    pub realized_pnl: f64,
    pub positions: HashMap<String, Position>,
    pub mid_prices: HashMap<String, f64>,
}

impl PortfolioSnapshot {
    pub fn with_starting_cash(cash: f64) -> Self {
        Self {
            cash,
            realized_pnl: 0.0,
            positions: HashMap::new(),
            mid_prices: HashMap::new(),
        }
    }

    pub fn inventory_for(&self, market: &str) -> f64 {
        self.positions.get(market).map(|p| p.qty).unwrap_or(0.0)
    }

    pub fn set_mid_price(&mut self, market: impl Into<String>, price: f64) {
        self.mid_prices.insert(market.into(), price);
    }

    pub fn unrealized_pnl(&self) -> f64 {
        self.positions
            .iter()
            .map(|(market, pos)| {
                if pos.qty.abs() < f64::EPSILON {
                    return 0.0;
                }
                let mid = self
                    .mid_prices
                    .get(market)
                    .copied()
                    .unwrap_or(pos.avg_price);
                if pos.qty > 0.0 {
                    (mid - pos.avg_price) * pos.qty
                } else {
                    (pos.avg_price - mid) * pos.qty.abs()
                }
            })
            .sum()
    }

    pub fn total_equity(&self) -> f64 {
        self.cash + self.unrealized_pnl()
    }
}
