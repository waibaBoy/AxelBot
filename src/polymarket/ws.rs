use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info};

use super::http::connect_ws_proxied;
use super::types::{WsBookDelta, WsMessage, WsSubscription, WsTradeEvent};
use crate::market_data::{MarketDataSource, MarketEvent};

// ---------------------------------------------------------------------------
// Live WebSocket market data source for Polymarket CLOB
// ---------------------------------------------------------------------------

/// Connects to the Polymarket CLOB WebSocket and streams real-time
/// orderbook updates and trades as `MarketEvent`s.
pub struct PolymarketWsSource {
    ws_url: String,
    token_ids: Vec<String>,
    /// Maps token_id → human-readable market identifier (condition_id or slug)
    token_to_market: std::collections::HashMap<String, String>,
    reader: Option<
        futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    >,
    writer: Option<
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    >,
    connected: bool,
    unknown_frame_logs: u8,
    log_full_unknown_frames: bool,
    use_legacy_subscribe_payload: bool,
    proxy_url: Option<String>,
}

impl PolymarketWsSource {
    /// Create a new WS source.
    ///
    /// # Arguments
    /// * `ws_url` - WebSocket endpoint (e.g. `wss://ws-subscriptions-clob.polymarket.com/ws/market`)
    /// * `token_ids` - List of CLOB token IDs to subscribe to
    /// * `token_to_market` - Mapping from token_id to a market identifier used internally
    pub fn new(
        ws_url: impl Into<String>,
        token_ids: Vec<String>,
        token_to_market: std::collections::HashMap<String, String>,
        proxy_url: Option<&str>,
    ) -> Self {
        Self {
            ws_url: ws_url.into(),
            token_ids,
            token_to_market,
            reader: None,
            writer: None,
            connected: false,
            unknown_frame_logs: 0,
            log_full_unknown_frames: env_flag("AXELBOT_WS_LOG_FULL_UNKNOWN"),
            use_legacy_subscribe_payload: env_flag("AXELBOT_WS_USE_LEGACY_SUB"),
            proxy_url: proxy_url.filter(|s| !s.is_empty()).map(|s| s.to_string()),
        }
    }

    /// Connect to the WebSocket and subscribe to market channel.
    pub async fn connect(&mut self) -> Result<()> {
        let (ws_stream, _response) =
            connect_ws_proxied(&self.ws_url, self.proxy_url.as_deref())
                .await
                .context("failed to connect to Polymarket WebSocket")?;

        let (writer, reader) = ws_stream.split();
        self.writer = Some(writer);
        self.reader = Some(reader);
        self.connected = true;

        // Send exactly one subscription payload. Sending both modern+legacy can
        // cause ambiguous server behavior in some deployments.
        let sub = if self.use_legacy_subscribe_payload {
            WsSubscription::market_legacy(self.token_ids.clone())
        } else {
            WsSubscription::market(self.token_ids.clone())
        };
        let msg = serde_json::to_string(&sub)?;
        self.send_raw(&msg).await?;

        debug!(
            "subscribed to market channel for {} token IDs",
            self.token_ids.len()
        );
        Ok(())
    }

    async fn send_raw(&mut self, msg: &str) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer
                .send(Message::Text(msg.into()))
                .await
                .context("failed to send WS message")?;
        }
        Ok(())
    }

    /// Read the next raw WS text frame, skipping pings/pongs.
    async fn next_text_frame(&mut self) -> Result<String> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| anyhow!("WebSocket not connected"))?;

        loop {
            match reader.next().await {
                Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
                Some(Ok(Message::Binary(bin))) => {
                    if let Ok(text) = String::from_utf8(bin.to_vec()) {
                        return Ok(text);
                    }
                    continue;
                }
                Some(Ok(Message::Ping(_))) => {
                    // Respond with pong via writer
                    if let Some(writer) = &mut self.writer {
                        let _ = writer.send(Message::Pong(vec![].into())).await;
                    }
                }
                Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) => {
                    self.connected = false;
                    return Err(anyhow!("WebSocket connection closed by server"));
                }
                Some(Ok(_)) => continue, // binary frames etc
                Some(Err(e)) => {
                    self.connected = false;
                    return Err(anyhow!("WebSocket error: {}", e));
                }
                None => {
                    self.connected = false;
                    return Err(anyhow!("WebSocket stream ended"));
                }
            }
        }
    }

    /// Parse a text frame into a MarketEvent, returning None for non-data messages.
    fn parse_frame(&mut self, text: &str) -> Option<MarketEvent> {
        // Legacy envelope support (channel + data)
        if let Ok(msg) = serde_json::from_str::<WsMessage>(text) {
            if let Some(event) = self.parse_legacy_envelope(msg) {
                return Some(event);
            }
        }

        // Current market-channel event format.
        let v: serde_json::Value = serde_json::from_str(text).ok()?;
        if let Some(items) = v.as_array() {
            for item in items {
                if let Some(event) = self.parse_value_event(item, text) {
                    return Some(event);
                }
            }
            return None;
        }
        self.parse_value_event(&v, text)
    }

    fn parse_value_event(&mut self, v: &serde_json::Value, raw_text: &str) -> Option<MarketEvent> {
        let event_type = v
            .get("event_type")
            .and_then(|x| x.as_str())
            .map(|s| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        match event_type.as_str() {
            "book" | "best_bid_ask" => self
                .parse_book_event_value(&v)
                .map(|evt| self.remap_market(evt)),
            "trade" | "last_trade_price" => self
                .parse_trade_event_value(&v)
                .map(|evt| self.remap_market(evt)),
            "price_change" => self
                .parse_price_change_value(v)
                .map(|evt| self.remap_market(evt)),
            "tick_size_change" | "new_market" | "market_resolved" => None,
            _ => {
                // Metadata frames sometimes arrive without event_type and do not
                // carry tradable token updates.
                if v.get("question").is_some()
                    && v.get("slug").is_some()
                    && v.get("market").is_some()
                    && v.get("asset_id").is_none()
                {
                    return None;
                }
                if self.unknown_frame_logs < 5 {
                    self.unknown_frame_logs += 1;
                    let snippet = if self.log_full_unknown_frames {
                        raw_text.to_string()
                    } else if raw_text.len() > 300 {
                        format!("{}...", &raw_text[..300])
                    } else {
                        raw_text.to_string()
                    };
                    info!(
                        event_type = %event_type,
                        payload_len = raw_text.len(),
                        snippet = %snippet,
                        "ws_unknown_frame"
                    );
                }
                // Legacy channels occasionally omit event_type and provide shape-only payloads.
                if v.get("bids").is_some()
                    || v.get("asks").is_some()
                    || (v.get("best_bid").is_some() && v.get("best_ask").is_some())
                    || (v.get("bestBid").is_some() && v.get("bestAsk").is_some())
                {
                    return self
                        .parse_book_event_value(v)
                        .map(|evt| self.remap_market(evt));
                }
                if v.get("price").is_some() || v.get("last_trade_price").is_some() {
                    return self
                        .parse_trade_event_value(v)
                        .map(|evt| self.remap_market(evt));
                }
                None
            }
        }
    }

    fn parse_legacy_envelope(&self, msg: WsMessage) -> Option<MarketEvent> {
        match msg.channel.as_deref() {
            Some("book") => {
                let data = msg.data?;
                let delta: WsBookDelta = serde_json::from_value(data).ok()?;
                delta.to_market_event().map(|evt| self.remap_market(evt))
            }
            Some("trade") => {
                let data = msg.data?;
                if let Ok(trade) = serde_json::from_value::<WsTradeEvent>(data.clone()) {
                    return trade.to_market_event().map(|evt| self.remap_market(evt));
                }
                if let Ok(trades) = serde_json::from_value::<Vec<WsTradeEvent>>(data) {
                    return trades
                        .into_iter()
                        .next()
                        .and_then(|t| t.to_market_event())
                        .map(|evt| self.remap_market(evt));
                }
                None
            }
            _ => None,
        }
    }

    fn parse_book_event_value(&self, v: &serde_json::Value) -> Option<MarketEvent> {
        let asset = v
            .get("asset_id")
            .or_else(|| v.get("assetId"))
            .and_then(value_to_string)?;
        let bids = v
            .get("bids")
            .or_else(|| v.get("buys"))
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        let asks = v
            .get("asks")
            .or_else(|| v.get("sells"))
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        let best_bid = bids
            .first()
            .and_then(level_price)
            .or_else(|| {
                v.get("best_bid")
                    .or_else(|| v.get("bestBid"))
                    .and_then(value_to_f64)
            })
            .unwrap_or(0.0);
        let best_ask = asks
            .first()
            .and_then(level_price)
            .or_else(|| {
                v.get("best_ask")
                    .or_else(|| v.get("bestAsk"))
                    .and_then(value_to_f64)
            })
            .unwrap_or(0.0);
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return None;
        }
        let best_bid_size = bids
            .first()
            .and_then(level_size)
            .or_else(|| {
                v.get("best_bid_size")
                    .or_else(|| v.get("bestBidSize"))
                    .and_then(value_to_f64)
            })
            .unwrap_or(0.0);
        let best_ask_size = asks
            .first()
            .and_then(level_size)
            .or_else(|| {
                v.get("best_ask_size")
                    .or_else(|| v.get("bestAskSize"))
                    .and_then(value_to_f64)
            })
            .unwrap_or(0.0);
        let ts = v
            .get("timestamp")
            .or_else(|| v.get("ts"))
            .and_then(value_to_dt)
            .unwrap_or_else(Utc::now);
        Some(MarketEvent::BookUpdate {
            market: asset,
            bid: best_bid,
            bid_size: best_bid_size,
            ask: best_ask,
            ask_size: best_ask_size,
            timestamp: ts,
        })
    }

    fn parse_trade_event_value(&self, v: &serde_json::Value) -> Option<MarketEvent> {
        let asset = v
            .get("asset_id")
            .or_else(|| v.get("assetId"))
            .and_then(value_to_string)?;
        let price = v
            .get("price")
            .or_else(|| v.get("last_trade_price"))
            .and_then(value_to_f64)?;
        let size = v.get("size").and_then(value_to_f64).unwrap_or(1.0);
        let side = v
            .get("side")
            .and_then(|x| x.as_str())
            .map(|s| s.to_ascii_uppercase())
            .map(|s| {
                if s == "BUY" {
                    crate::types::Side::Bid
                } else {
                    crate::types::Side::Ask
                }
            })
            .unwrap_or(crate::types::Side::Bid);
        let ts = v
            .get("timestamp")
            .or_else(|| v.get("ts"))
            .and_then(value_to_dt)
            .unwrap_or_else(Utc::now);
        Some(MarketEvent::Trade {
            market: asset,
            price,
            size,
            side,
            timestamp: ts,
        })
    }

    fn parse_price_change_value(&self, v: &serde_json::Value) -> Option<MarketEvent> {
        let changes = v.get("price_changes")?.as_array()?;
        let ts = v
            .get("timestamp")
            .or_else(|| v.get("ts"))
            .and_then(value_to_dt)
            .unwrap_or_else(Utc::now);

        for change in changes {
            let asset = change.get("asset_id").and_then(value_to_string)?;
            let best_bid = change
                .get("best_bid")
                .or_else(|| change.get("bestBid"))
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            let best_ask = change
                .get("best_ask")
                .or_else(|| change.get("bestAsk"))
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            if best_bid > 0.0 && best_ask > 0.0 && best_ask >= best_bid {
                let level_size = change.get("size").and_then(value_to_f64).unwrap_or(1.0);
                let side = change
                    .get("side")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_ascii_uppercase())
                    .unwrap_or_default();
                let (bid_size, ask_size) = if side == "BUY" {
                    (level_size.max(0.0), 1.0)
                } else if side == "SELL" {
                    (1.0, level_size.max(0.0))
                } else {
                    (1.0, 1.0)
                };
                return Some(MarketEvent::BookUpdate {
                    market: asset,
                    bid: best_bid,
                    bid_size,
                    ask: best_ask,
                    ask_size,
                    timestamp: ts,
                });
            }

            // Fallback: treat as trade-like update.
            let price = change.get("price").and_then(value_to_f64)?;
            let size = change.get("size").and_then(value_to_f64).unwrap_or(1.0);
            let side = change
                .get("side")
                .and_then(|x| x.as_str())
                .map(|s| s.to_ascii_uppercase())
                .map(|s| {
                    if s == "BUY" {
                        crate::types::Side::Bid
                    } else {
                        crate::types::Side::Ask
                    }
                })
                .unwrap_or(crate::types::Side::Bid);
            return Some(MarketEvent::Trade {
                market: asset,
                price,
                size,
                side,
                timestamp: ts,
            });
        }
        None
    }

    fn remap_market(&self, event: MarketEvent) -> MarketEvent {
        match event {
            MarketEvent::BookUpdate {
                market,
                bid,
                bid_size,
                ask,
                ask_size,
                timestamp,
            } => {
                let mapped = self.token_to_market.get(&market).cloned().unwrap_or(market);
                MarketEvent::BookUpdate {
                    market: mapped,
                    bid,
                    bid_size,
                    ask,
                    ask_size,
                    timestamp,
                }
            }
            MarketEvent::Trade {
                market,
                price,
                size,
                side,
                timestamp,
            } => {
                let mapped = self.token_to_market.get(&market).cloned().unwrap_or(market);
                MarketEvent::Trade {
                    market: mapped,
                    price,
                    size,
                    side,
                    timestamp,
                }
            }
        }
    }
}

fn level_price(level: &serde_json::Value) -> Option<f64> {
    if let Some(arr) = level.as_array() {
        return arr.first().and_then(value_to_f64);
    }
    if let Some(obj) = level.as_object() {
        if let Some(v) = obj.get("price").and_then(value_to_f64) {
            return Some(v);
        }
        if let Some(v) = obj.get("p").and_then(value_to_f64) {
            return Some(v);
        }
    }
    None
}

fn level_size(level: &serde_json::Value) -> Option<f64> {
    if let Some(arr) = level.as_array() {
        return arr.get(1).and_then(value_to_f64);
    }
    if let Some(obj) = level.as_object() {
        if let Some(v) = obj.get("size").and_then(value_to_f64) {
            return Some(v);
        }
        if let Some(v) = obj.get("s").and_then(value_to_f64) {
            return Some(v);
        }
    }
    None
}

fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(n) = v.as_i64() {
        return Some(n as f64);
    }
    if let Some(n) = v.as_u64() {
        return Some(n as f64);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

fn value_to_string(v: &serde_json::Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(n) = v.as_u64() {
        return Some(n.to_string());
    }
    if let Some(n) = v.as_i64() {
        return Some(n.to_string());
    }
    None
}

fn value_to_dt(v: &serde_json::Value) -> Option<DateTime<Utc>> {
    if let Some(n) = v.as_i64() {
        if n > 1_000_000_000_000 {
            return Utc.timestamp_millis_opt(n).single();
        }
        return Utc.timestamp_opt(n, 0).single();
    }
    if let Some(s) = v.as_str() {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.with_timezone(&Utc));
        }
        if let Ok(n) = s.parse::<i64>() {
            if n > 1_000_000_000_000 {
                return Utc.timestamp_millis_opt(n).single();
            }
            return Utc.timestamp_opt(n, 0).single();
        }
    }
    None
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

#[async_trait]
impl MarketDataSource for PolymarketWsSource {
    async fn next_event(&mut self) -> Result<MarketEvent> {
        if !self.connected {
            self.connect().await?;
        }

        loop {
            let text = self.next_text_frame().await?;
            if let Some(event) = self.parse_frame(&text) {
                return Ok(event);
            }
            // Non-data frame, loop and read next
        }
    }
}
