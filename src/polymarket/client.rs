use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::Sha256;

use super::signer::{OrderParams, PolymarketSigner};
use super::types::{
    ClobFeeRate, ClobMarket, ClobOpenOrder, ClobOrderRequest, ClobOrderResponse, ClobOrderType,
    ClobOrderbook, ClobServerTime, ClobSide, ClobTickSize, ClobTrade, PriceHistoryPoint,
};
use crate::types::Side;

use alloy_primitives::U256;

// ---------------------------------------------------------------------------
// HMAC-SHA256 API authentication (Polymarket CLOB L2 auth)
// ---------------------------------------------------------------------------

type HmacSha256 = Hmac<Sha256>;

/// Polymarket CLOB REST API client.
///
/// Handles HMAC authentication, order signing, and all REST endpoints.
pub struct PolymarketClient {
    http: reqwest::Client,
    rest_url: String,
    api_key: String,
    api_secret: String,
    passphrase: String,
    signer: PolymarketSigner,
    wallet_address: String,
    /// Fee rate in basis points applied to orders
    fee_rate_bps: u64,
}

/// Public/read-only Polymarket CLOB client for market data and metadata endpoints.
pub struct PolymarketPublicClient {
    http: reqwest::Client,
    rest_url: String,
}

impl PolymarketPublicClient {
    pub fn new(rest_url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            rest_url: rest_url.into(),
        }
    }

    pub async fn get_server_time(&self) -> Result<ClobServerTime> {
        let ts = get_server_time_ms(&self.http, &format!("{}/time", self.rest_url)).await?;
        Ok(ClobServerTime { epoch_ms: ts })
    }

    /// Fetch the orderbook for a token without authentication.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<ClobOrderbook> {
        let url = format!("{}/book?token_id={}", self.rest_url, token_id);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("GET /book failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("GET /book returned {}: {}", status, body));
        }
        resp.json().await.context("failed to parse orderbook")
    }

    /// Fetch all markets from the CLOB public endpoint.
    pub async fn get_markets(
        &self,
        next_cursor: Option<&str>,
    ) -> Result<(Vec<ClobMarket>, Option<String>)> {
        let mut url = format!("{}/markets", self.rest_url);
        if let Some(cursor) = next_cursor {
            url = format!("{}?next_cursor={}", url, cursor);
        }
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("GET /markets failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("GET /markets returned {}: {}", status, body));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .context("failed to parse /markets response")?;
        let markets: Vec<ClobMarket> = if let Some(data) = json.get("data") {
            serde_json::from_value(data.clone()).unwrap_or_default()
        } else if json.is_array() {
            serde_json::from_value(json.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };

        let cursor = json
            .get("next_cursor")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && *s != "LTE=")
            .map(|s| s.to_string());

        Ok((markets, cursor))
    }

    /// Fetch active/non-closed markets with pagination.
    pub async fn get_all_active_markets(&self, max_markets: usize) -> Result<Vec<ClobMarket>> {
        let mut all_markets = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let (markets, next) = self.get_markets(cursor.as_deref()).await?;
            if markets.is_empty() {
                break;
            }
            for m in markets {
                if m.active && !m.closed {
                    all_markets.push(m);
                    if all_markets.len() >= max_markets {
                        return Ok(all_markets);
                    }
                }
            }
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(all_markets)
    }

    /// Fetch recently traded asset IDs to prioritize live, active tokens.
    pub async fn get_recent_trade_asset_ids(&self, limit: usize) -> Result<Vec<String>> {
        let urls = [
            format!("{}/trades", self.rest_url),
            format!("{}/data/trades", self.rest_url),
        ];
        let mut last_err = None;
        for url in urls {
            match get_json(&self.http, &url, "GET /trades").await {
                Ok(json) => {
                    let mut out = Vec::new();
                    let rows: Vec<serde_json::Value> = if let Some(arr) = json.as_array() {
                        arr.clone()
                    } else if let Some(arr) = json.get("data").and_then(|v| v.as_array()) {
                        arr.clone()
                    } else {
                        Vec::new()
                    };
                    for row in rows {
                        let id = row
                            .get("asset_id")
                            .or_else(|| row.get("assetId"))
                            .or_else(|| row.get("token_id"))
                            .or_else(|| row.get("tokenId"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .trim()
                            .to_string();
                        if id.is_empty() {
                            continue;
                        }
                        if !out.contains(&id) {
                            out.push(id);
                            if out.len() >= limit {
                                break;
                            }
                        }
                    }
                    return Ok(out);
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                }
            }
        }
        Err(anyhow!(
            "failed to fetch recent trades: {}",
            last_err.unwrap_or_else(|| "unknown error".to_string())
        ))
    }

    pub async fn get_fee_rate(&self) -> Result<ClobFeeRate> {
        let json = get_json(
            &self.http,
            &format!("{}/fee-rate", self.rest_url),
            "GET /fee-rate",
        )
        .await?;
        let fee = extract_numeric(
            &json,
            &[
                "fee_rate_bps",
                "feeRateBps",
                "makerFeeRateBps",
                "base_fee",
                "baseFee",
            ],
        )
        .context("missing fee_rate_bps in /fee-rate response")?;
        Ok(ClobFeeRate { fee_rate_bps: fee })
    }

    pub async fn get_fee_rate_for_token(&self, token_id: &str) -> Result<ClobFeeRate> {
        let urls = [
            format!("{}/fee-rate/{}", self.rest_url, token_id),
            format!("{}/fee-rate?token_id={}", self.rest_url, token_id),
            format!("{}/fee-rate?asset_id={}", self.rest_url, token_id),
        ];
        for (idx, url) in urls.iter().enumerate() {
            if let Ok(json) = get_json(
                &self.http,
                url,
                match idx {
                    0 => "GET /fee-rate/{token_id}",
                    1 => "GET /fee-rate?token_id=...",
                    _ => "GET /fee-rate?asset_id=...",
                },
            )
            .await
            {
                if let Some(fee) = extract_numeric(
                    &json,
                    &[
                        "fee_rate_bps",
                        "feeRateBps",
                        "makerFeeRateBps",
                        "base_fee",
                        "baseFee",
                    ],
                ) {
                    return Ok(ClobFeeRate { fee_rate_bps: fee });
                }
            }
        }
        Err(anyhow!(
            "unable to fetch fee rate for token_id {}; tried path and query variants",
            token_id
        ))
    }

    pub async fn get_tick_size(&self) -> Result<ClobTickSize> {
        let json = get_json(
            &self.http,
            &format!("{}/tick-size", self.rest_url),
            "GET /tick-size",
        )
        .await?;
        let tick = extract_numeric(
            &json,
            &[
                "tick_size",
                "tickSize",
                "minimum_tick_size",
                "minimumTickSize",
            ],
        )
        .context("missing tick_size in /tick-size response")?;
        Ok(ClobTickSize { tick_size: tick })
    }

    pub async fn get_tick_size_for_token(&self, token_id: &str) -> Result<ClobTickSize> {
        let urls = [
            format!("{}/tick-size/{}", self.rest_url, token_id),
            format!("{}/tick-size?token_id={}", self.rest_url, token_id),
            format!("{}/tick-size?asset_id={}", self.rest_url, token_id),
        ];
        for (idx, url) in urls.iter().enumerate() {
            if let Ok(json) = get_json(
                &self.http,
                url,
                match idx {
                    0 => "GET /tick-size/{token_id}",
                    1 => "GET /tick-size?token_id=...",
                    _ => "GET /tick-size?asset_id=...",
                },
            )
            .await
            {
                if let Some(tick) = extract_numeric(
                    &json,
                    &[
                        "tick_size",
                        "tickSize",
                        "minimum_tick_size",
                        "minimumTickSize",
                    ],
                ) {
                    return Ok(ClobTickSize { tick_size: tick });
                }
            }
        }
        Err(anyhow!(
            "unable to fetch tick size for token_id {}; tried path and query variants",
            token_id
        ))
    }

    pub async fn get_prices_history(
        &self,
        token_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        fidelity: &str,
    ) -> Result<Vec<PriceHistoryPoint>> {
        let start_s = start.timestamp();
        let end_s = end.timestamp();
        let fidelity_lc = fidelity.trim().to_ascii_lowercase();
        let interval_mode = matches!(fidelity_lc.as_str(), "all" | "max");
        let fidelity_minutes = match parse_fidelity_minutes(&fidelity_lc) {
            Some(v) => v.to_string(),
            None => "1".to_string(),
        };

        let urls: Vec<String> = if interval_mode {
            vec![format!(
                "{}/prices-history?market={}&interval={}",
                self.rest_url, token_id, fidelity_lc
            )]
        } else {
            vec![
                // Official client/docs shape: token ID under `market`, unix seconds.
                format!(
                    "{}/prices-history?market={}&startTs={}&endTs={}&fidelity={}",
                    self.rest_url, token_id, start_s, end_s, fidelity_minutes
                ),
                // Compatibility fallback for older variants expecting `asset_id`.
                format!(
                    "{}/prices-history?asset_id={}&startTs={}&endTs={}&fidelity={}",
                    self.rest_url, token_id, start_s, end_s, fidelity_minutes
                ),
            ]
        };

        let mut errs: Vec<String> = Vec::new();
        for url in urls {
            match get_json(&self.http, &url, "GET /prices-history").await {
                Ok(json) => match parse_price_history(&json) {
                    Ok(points) => return Ok(points),
                    Err(e) => errs.push(e.to_string()),
                },
                Err(e) => errs.push(e.to_string()),
            }
        }
        Err(anyhow!(
            "unable to fetch/parse prices history for token {}; errors: {}",
            token_id,
            if errs.is_empty() {
                "unknown error".to_string()
            } else {
                errs.join(" | ")
            }
        ))
    }
}

fn parse_fidelity_minutes(input: &str) -> Option<u32> {
    if let Ok(v) = input.parse::<u32>() {
        return Some(v);
    }
    match input {
        "1m" => Some(1),
        "5m" => Some(5),
        "15m" => Some(15),
        "30m" => Some(30),
        "1h" => Some(60),
        "4h" => Some(240),
        "6h" => Some(360),
        "1d" => Some(1440),
        "1w" => Some(10080),
        _ => None,
    }
}

impl PolymarketClient {
    /// Create a new client.
    ///
    /// # Arguments
    /// * `rest_url` - Base URL for the CLOB REST API (e.g. `https://clob.polymarket.com`)
    /// * `api_key` - CLOB API key
    /// * `api_secret` - CLOB API secret (base64-encoded)
    /// * `passphrase` - CLOB API passphrase
    /// * `signer` - EIP-712 order signer
    /// * `wallet_address` - Hex-encoded wallet address (e.g. `0xabc...`)
    pub fn new(
        rest_url: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        passphrase: impl Into<String>,
        signer: PolymarketSigner,
        wallet_address: impl Into<String>,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            rest_url: rest_url.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            passphrase: passphrase.into(),
            signer,
            wallet_address: wallet_address.into(),
            fee_rate_bps: 0,
        }
    }

    pub fn set_fee_rate_bps(&mut self, fee_rate_bps: u64) {
        self.fee_rate_bps = fee_rate_bps;
    }

    /// Return cloned API credentials for authenticated WebSocket user channel usage.
    pub fn user_ws_auth(&self) -> (String, String, String) {
        (
            self.api_key.clone(),
            self.api_secret.clone(),
            self.passphrase.clone(),
        )
    }

    // -----------------------------------------------------------------------
    // Authentication
    // -----------------------------------------------------------------------

    /// Build HMAC-SHA256 authentication headers for an API request.
    fn auth_headers(&self, method: &str, path: &str, body: &str) -> Result<HeaderMap> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock error")?
            .as_secs()
            .to_string();

        let message = format!("{}{}{}{}", timestamp, method.to_uppercase(), path, body);

        let secret_bytes =
            base64_decode(&self.api_secret).context("failed to decode API secret")?;
        let mut mac =
            HmacSha256::new_from_slice(&secret_bytes).context("invalid HMAC key length")?;
        mac.update(message.as_bytes());
        let signature = base64_encode(&mac.finalize().into_bytes());

        let mut headers = HeaderMap::new();
        headers.insert("POLY_ADDRESS", HeaderValue::from_str(&self.wallet_address)?);
        headers.insert("POLY_SIGNATURE", HeaderValue::from_str(&signature)?);
        headers.insert("POLY_TIMESTAMP", HeaderValue::from_str(&timestamp)?);
        headers.insert("POLY_NONCE", HeaderValue::from_str(&timestamp)?);
        headers.insert("POLY_API_KEY", HeaderValue::from_str(&self.api_key)?);
        headers.insert("POLY_PASSPHRASE", HeaderValue::from_str(&self.passphrase)?);
        Ok(headers)
    }

    // -----------------------------------------------------------------------
    // Market Discovery
    // -----------------------------------------------------------------------

    /// Fetch all active markets from the CLOB.
    /// Returns markets sorted by volume/activity (if the API supports it).
    pub async fn get_markets(
        &self,
        next_cursor: Option<&str>,
    ) -> Result<(Vec<ClobMarket>, Option<String>)> {
        let mut url = format!("{}/markets", self.rest_url);
        if let Some(cursor) = next_cursor {
            url = format!("{}?next_cursor={}", url, cursor);
        }
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("GET /markets failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("GET /markets returned {}: {}", status, body));
        }

        // The API returns { data: [...], next_cursor: "..." }
        let json: serde_json::Value = resp
            .json()
            .await
            .context("failed to parse /markets response")?;

        let markets: Vec<ClobMarket> = if let Some(data) = json.get("data") {
            serde_json::from_value(data.clone()).unwrap_or_default()
        } else if json.is_array() {
            serde_json::from_value(json.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };

        let cursor = json
            .get("next_cursor")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && *s != "LTE=")
            .map(|s| s.to_string());

        Ok((markets, cursor))
    }

    /// Fetch all active, non-closed markets by paginating through the API.
    pub async fn get_all_active_markets(&self) -> Result<Vec<ClobMarket>> {
        let mut all_markets = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let (markets, next) = self.get_markets(cursor.as_deref()).await?;
            if markets.is_empty() {
                break;
            }
            for m in markets {
                if m.active && !m.closed {
                    all_markets.push(m);
                }
            }
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(all_markets)
    }

    /// Fetch a single market by condition_id.
    pub async fn get_market(&self, condition_id: &str) -> Result<ClobMarket> {
        let url = format!("{}/markets/{}", self.rest_url, condition_id);
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "GET /markets/{} returned {}: {}",
                condition_id,
                status,
                body
            ));
        }
        resp.json().await.context("failed to parse market response")
    }

    // -----------------------------------------------------------------------
    // Orderbook
    // -----------------------------------------------------------------------

    /// Fetch the orderbook for a given token_id.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<ClobOrderbook> {
        let url = format!("{}/book?token_id={}", self.rest_url, token_id);
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("GET /book returned {}: {}", status, body));
        }
        resp.json().await.context("failed to parse orderbook")
    }

    /// Fetch CLOB server time in epoch milliseconds.
    pub async fn get_server_time(&self) -> Result<ClobServerTime> {
        PolymarketPublicClient::new(self.rest_url.clone())
            .get_server_time()
            .await
    }

    /// Fetch current default fee rate in basis points.
    pub async fn get_fee_rate(&self) -> Result<ClobFeeRate> {
        PolymarketPublicClient::new(self.rest_url.clone())
            .get_fee_rate()
            .await
    }

    /// Fetch fee rate for a specific token.
    pub async fn get_fee_rate_for_token(&self, token_id: &str) -> Result<ClobFeeRate> {
        PolymarketPublicClient::new(self.rest_url.clone())
            .get_fee_rate_for_token(token_id)
            .await
    }

    /// Fetch default tick size.
    pub async fn get_tick_size(&self) -> Result<ClobTickSize> {
        PolymarketPublicClient::new(self.rest_url.clone())
            .get_tick_size()
            .await
    }

    /// Fetch tick size for a specific token.
    pub async fn get_tick_size_for_token(&self, token_id: &str) -> Result<ClobTickSize> {
        PolymarketPublicClient::new(self.rest_url.clone())
            .get_tick_size_for_token(token_id)
            .await
    }

    /// Fetch historical prices for replay/backtesting.
    pub async fn get_prices_history(
        &self,
        token_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        fidelity: &str,
    ) -> Result<Vec<PriceHistoryPoint>> {
        PolymarketPublicClient::new(self.rest_url.clone())
            .get_prices_history(token_id, start, end, fidelity)
            .await
    }

    // -----------------------------------------------------------------------
    // Order Placement
    // -----------------------------------------------------------------------

    /// Place a signed order on the CLOB.
    ///
    /// This method:
    /// 1. Signs the order using EIP-712
    /// 2. Wraps it in a ClobOrderRequest
    /// 3. Sends it with HMAC auth headers
    pub async fn place_order(
        &self,
        token_id: &str,
        side: Side,
        price: f64,
        size: f64,
        neg_risk: bool,
        nonce: U256,
        order_type: ClobOrderType,
    ) -> Result<ClobOrderResponse> {
        let clob_side = match side {
            Side::Bid => ClobSide::Buy,
            Side::Ask => ClobSide::Sell,
        };

        // Polymarket uses USDC with 6 decimals.
        // For BUY: maker_amount = price * size * 1e6 (USDC you pay), taker_amount = size * 1e6 (shares you get)
        // For SELL: maker_amount = size * 1e6 (shares you give), taker_amount = price * size * 1e6 (USDC you get)
        let (maker_amount, taker_amount) = match clob_side {
            ClobSide::Buy => {
                let usdc = (price * size * 1_000_000.0).round() as u64;
                let shares = (size * 1_000_000.0).round() as u64;
                (U256::from(usdc), U256::from(shares))
            }
            ClobSide::Sell => {
                let shares = (size * 1_000_000.0).round() as u64;
                let usdc = (price * size * 1_000_000.0).round() as u64;
                (U256::from(shares), U256::from(usdc))
            }
        };

        let params = OrderParams {
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
            side: clob_side,
            fee_rate_bps: self.fee_rate_bps,
            nonce,
            expiration: U256::ZERO, // no expiration
        };

        let signed = self
            .signer
            .sign_order(&params, neg_risk)
            .context("failed to sign order")?;

        let request = ClobOrderRequest {
            order: signed,
            order_type,
            owner: self.wallet_address.clone(),
        };

        let body = serde_json::to_string(&request).context("failed to serialize order")?;
        let path = "/order";
        let headers = self.auth_headers("POST", path, &body)?;

        let url = format!("{}{}", self.rest_url, path);
        let resp = self
            .http
            .post(&url)
            .headers(headers)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .context("POST /order failed")?;

        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!("POST /order returned {}: {}", status, resp_body));
        }

        serde_json::from_str(&resp_body).context("failed to parse order response")
    }

    // -----------------------------------------------------------------------
    // Order Cancellation
    // -----------------------------------------------------------------------

    /// Cancel an order by order ID.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let body = serde_json::json!({ "orderID": order_id }).to_string();
        let path = "/order";
        let headers = self.auth_headers("DELETE", path, &body)?;

        let url = format!("{}{}", self.rest_url, path);
        let resp = self
            .http
            .delete(&url)
            .headers(headers)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .context("DELETE /order failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DELETE /order returned {}: {}", status, body));
        }
        Ok(())
    }

    /// Cancel all open orders.
    pub async fn cancel_all(&self) -> Result<()> {
        let path = "/cancel-all";
        let headers = self.auth_headers("DELETE", path, "")?;
        let url = format!("{}{}", self.rest_url, path);
        let resp = self
            .http
            .delete(&url)
            .headers(headers)
            .send()
            .await
            .context("DELETE /cancel-all failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("DELETE /cancel-all returned {}: {}", status, body));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Open Orders & Trades
    // -----------------------------------------------------------------------

    /// Fetch open orders, optionally filtered by market.
    pub async fn get_open_orders(&self, market: Option<&str>) -> Result<Vec<ClobOpenOrder>> {
        let mut url = format!("{}/orders", self.rest_url);
        if let Some(m) = market {
            url = format!("{}?market={}", url, m);
        }
        let path = if let Some(m) = market {
            format!("/orders?market={}", m)
        } else {
            "/orders".to_string()
        };
        let headers = self.auth_headers("GET", &path, "")?;

        let resp = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /orders failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("GET /orders returned {}: {}", status, body));
        }
        resp.json().await.context("failed to parse open orders")
    }

    /// Fetch recent trades for the authenticated user.
    pub async fn get_trades(&self, market: Option<&str>) -> Result<Vec<ClobTrade>> {
        let mut url = format!("{}/data/trades", self.rest_url);
        if let Some(m) = market {
            url = format!("{}?market={}", url, m);
        }
        let path = if let Some(m) = market {
            format!("/data/trades?market={}", m)
        } else {
            "/data/trades".to_string()
        };
        let headers = self.auth_headers("GET", &path, "")?;

        let resp = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /data/trades failed")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("GET /data/trades returned {}: {}", status, body));
        }
        resp.json().await.context("failed to parse trades")
    }
}

// ---------------------------------------------------------------------------
// Base64 helpers (avoid pulling in another crate just for this)
// ---------------------------------------------------------------------------

fn base64_decode(input: &str) -> Result<Vec<u8>> {
    // Standard base64 decoding
    let table: Vec<u8> =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".to_vec();
    let mut output = Vec::new();
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;

    for &byte in input.as_bytes() {
        if byte == b'=' {
            break;
        }
        let val = table.iter().position(|&b| b == byte);
        let val = match val {
            Some(v) => v as u32,
            None => continue, // skip whitespace etc
        };
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Ok(output)
}

fn base64_encode(input: &[u8]) -> String {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as u32;
        let b1 = if i + 1 < input.len() {
            input[i + 1] as u32
        } else {
            0
        };
        let b2 = if i + 2 < input.len() {
            input[i + 2] as u32
        } else {
            0
        };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(table[((triple >> 18) & 0x3F) as usize] as char);
        result.push(table[((triple >> 12) & 0x3F) as usize] as char);
        if i + 1 < input.len() {
            result.push(table[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if i + 2 < input.len() {
            result.push(table[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        i += 3;
    }
    result
}

async fn get_json(http: &reqwest::Client, url: &str, context: &str) -> Result<serde_json::Value> {
    let resp = http
        .get(url)
        .send()
        .await
        .with_context(|| format!("{context} failed"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("{context} returned {}: {}", status, body));
    }
    resp.json()
        .await
        .with_context(|| format!("failed to parse JSON for {context}"))
}

async fn get_server_time_ms(http: &reqwest::Client, url: &str) -> Result<i64> {
    let resp = http.get(url).send().await.context("GET /time failed")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("GET /time returned {}: {}", status, body));
    }

    let trimmed = body.trim();
    if let Ok(n) = trimmed.parse::<i64>() {
        return Ok(if n < 10_000_000_000 { n * 1000 } else { n });
    }

    let json: serde_json::Value =
        serde_json::from_str(trimmed).context("failed to parse /time response")?;
    extract_timestamp_ms(&json).context("missing server time in /time response")
}

fn extract_numeric(json: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = json.get(key) {
            if let Some(n) = value_as_f64(v) {
                return Some(n);
            }
        }
    }
    if let Some(obj) = json.get("data") {
        for key in keys {
            if let Some(v) = obj.get(*key) {
                if let Some(n) = value_as_f64(v) {
                    return Some(n);
                }
            }
        }
    }
    None
}

fn extract_timestamp_ms(json: &serde_json::Value) -> Option<i64> {
    let keys = ["timestamp", "time", "serverTime", "server_time", "epoch_ms"];
    for key in keys {
        if let Some(v) = json.get(key) {
            if let Some(ts) = value_as_timestamp_ms(v) {
                return Some(ts);
            }
        }
    }
    if let Some(obj) = json.get("data") {
        for key in keys {
            if let Some(v) = obj.get(key) {
                if let Some(ts) = value_as_timestamp_ms(v) {
                    return Some(ts);
                }
            }
        }
    }
    None
}

fn parse_price_history(json: &serde_json::Value) -> Result<Vec<PriceHistoryPoint>> {
    let rows = if let Some(arr) = json.as_array() {
        arr
    } else if let Some(arr) = json.get("history").and_then(|v| v.as_array()) {
        arr
    } else if let Some(arr) = json.get("data").and_then(|v| v.as_array()) {
        arr
    } else if let Some(arr) = json.get("prices").and_then(|v| v.as_array()) {
        arr
    } else if let Some(arr) = json
        .get("data")
        .and_then(|v| v.get("history"))
        .and_then(|v| v.as_array())
    {
        arr
    } else {
        return Err(anyhow!(
            "unable to locate price history array in /prices-history response"
        ));
    };

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let ts = extract_timestamp_ms(row)
            .or_else(|| row.get("t").and_then(value_as_timestamp_ms))
            .or_else(|| row.get("ts").and_then(value_as_timestamp_ms));
        let price = extract_numeric(row, &["price", "p", "mid", "last"]);
        if let (Some(ts), Some(price)) = (ts, price) {
            let dt = Utc
                .timestamp_millis_opt(ts)
                .single()
                .ok_or_else(|| anyhow!("invalid timestamp in price history: {ts}"))?;
            out.push(PriceHistoryPoint {
                timestamp: dt,
                price,
            });
        }
    }
    out.sort_by_key(|p| p.timestamp.timestamp_millis());
    out.dedup_by_key(|p| p.timestamp.timestamp_millis());
    Ok(out)
}

fn value_as_timestamp_ms(v: &serde_json::Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(if n < 10_000_000_000 { n * 1000 } else { n });
    }
    if let Some(n) = v.as_u64() {
        let i = n as i64;
        return Some(if i < 10_000_000_000 { i * 1000 } else { i });
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Some(if n < 10_000_000_000 { n * 1000 } else { n });
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
    }
    None
}

fn value_as_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(f) = v.as_f64() {
        return Some(f);
    }
    if let Some(i) = v.as_i64() {
        return Some(i as f64);
    }
    if let Some(u) = v.as_u64() {
        return Some(u as f64);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}
