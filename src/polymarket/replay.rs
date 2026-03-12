use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Duration;

use crate::market_data::{MarketDataSource, MarketEvent};

use super::types::PriceHistoryPoint;

/// Converts historical price points into synthetic market events for replay backtests.
pub struct HistoricalReplaySource {
    events: Vec<MarketEvent>,
    cursor: usize,
}

impl HistoricalReplaySource {
    pub fn from_price_history(
        market: impl Into<String>,
        points: Vec<PriceHistoryPoint>,
        synthetic_spread_bps: f64,
    ) -> Self {
        let market = market.into();
        let mut events = Vec::with_capacity(points.len());
        let spread_fraction = (synthetic_spread_bps / 10_000.0).max(0.00001);

        for point in points {
            let mid = point.price.clamp(0.01, 0.99);
            let bid = (mid * (1.0 - spread_fraction * 0.5)).clamp(0.01, 0.99);
            let ask = (mid * (1.0 + spread_fraction * 0.5)).clamp(0.01, 0.99);
            events.push(MarketEvent::BookUpdate {
                market: market.clone(),
                bid,
                bid_size: 10.0,
                ask: ask.max(bid + 0.0001).clamp(0.01, 0.99),
                ask_size: 10.0,
                timestamp: point.timestamp,
            });
        }

        // Insert tiny temporal gaps when points have identical timestamps.
        let mut shifted = Vec::with_capacity(events.len());
        let mut last_ts = None;
        for event in events {
            let event = match event {
                MarketEvent::BookUpdate {
                    market,
                    bid,
                    bid_size,
                    ask,
                    ask_size,
                    timestamp,
                } => {
                    let adjusted = if last_ts == Some(timestamp) {
                        timestamp + Duration::milliseconds(1)
                    } else {
                        timestamp
                    };
                    last_ts = Some(adjusted);
                    MarketEvent::BookUpdate {
                        market,
                        bid,
                        bid_size,
                        ask,
                        ask_size,
                        timestamp: adjusted,
                    }
                }
                other => other,
            };
            shifted.push(event);
        }

        Self {
            events: shifted,
            cursor: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}

#[async_trait]
impl MarketDataSource for HistoricalReplaySource {
    async fn next_event(&mut self) -> Result<MarketEvent> {
        let event = self
            .events
            .get(self.cursor)
            .cloned()
            .ok_or_else(|| anyhow!("historical replay exhausted"))?;
        self.cursor += 1;
        Ok(event)
    }
}
