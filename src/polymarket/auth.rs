use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::Deserialize;

use super::http::build_http_client;

const EIP712_DOMAIN_TYPEHASH: &[u8] = b"EIP712Domain(string name,string version,uint256 chainId)";
const CLOB_AUTH_TYPEHASH: &[u8] =
    b"ClobAuth(address address,string timestamp,uint256 nonce,string message)";
const CLOB_AUTH_DOMAIN_NAME: &str = "ClobAuthDomain";
const CLOB_AUTH_DOMAIN_VERSION: &str = "1";
const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

#[derive(Debug, Clone, Copy)]
pub enum ApiCredsMode {
    Create,
    Derive,
    CreateOrDerive,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiCredentials {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.body)
    }
}

impl std::error::Error for ApiError {}

pub async fn get_api_credentials(
    rest_url: &str,
    private_key: &str,
    chain_id: u64,
    nonce: u64,
    mode: ApiCredsMode,
    proxy_url: Option<&str>,
) -> Result<(String, ApiCredentials)> {
    let signer = parse_signer(private_key)?;
    let address = format!("{:?}", signer.address());
    let ts = get_server_timestamp_secs(rest_url, proxy_url).await?;
    let signature = sign_l1_auth(&signer, chain_id, &address, &ts, nonce)?;
    let headers = build_l1_headers(&address, &signature, &ts, nonce)?;

    let creds = match mode {
        ApiCredsMode::Create => {
            request_api_credentials(rest_url, Method::POST, "/auth/api-key", headers, proxy_url).await?
        }
        ApiCredsMode::Derive => {
            request_api_credentials(rest_url, Method::GET, "/auth/derive-api-key", headers, proxy_url).await?
        }
        ApiCredsMode::CreateOrDerive => {
            match request_api_credentials(rest_url, Method::POST, "/auth/api-key", headers.clone(), proxy_url)
                .await
            {
                Ok(c) => c,
                Err(err)
                    if err.status == StatusCode::CONFLICT
                        || err.status == StatusCode::BAD_REQUEST
                        || err.body.to_ascii_uppercase().contains("NONCE_ALREADY_USED") =>
                {
                    request_api_credentials(rest_url, Method::GET, "/auth/derive-api-key", headers, proxy_url)
                        .await?
                }
                Err(err) => return Err(anyhow!("failed to create API key: {err}")),
            }
        }
    };

    Ok((address, creds))
}

fn parse_signer(private_key: &str) -> Result<PrivateKeySigner> {
    let key_hex = private_key
        .trim()
        .strip_prefix("0x")
        .unwrap_or(private_key.trim());
    if key_hex.len() != 64 {
        return Err(anyhow!(
            "failed to parse wallet private key: expected 64 hex chars (32 bytes), got {}",
            key_hex.len()
        ));
    }
    if !key_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!(
            "failed to parse wallet private key: non-hex characters detected"
        ));
    }
    key_hex
        .parse()
        .context("failed to parse wallet private key")
}

async fn get_server_timestamp_secs(rest_url: &str, proxy_url: Option<&str>) -> Result<String> {
    let rest_url = rest_url.trim_end_matches('/');
    let url = format!("{rest_url}/time");
    let resp = build_http_client(proxy_url)?
        .get(url)
        .send()
        .await
        .context("GET /time failed")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("GET /time returned {}: {}", status, body));
    }

    // Some CLOB deployments return plain text unix time (e.g. "1773328637"),
    // while others return JSON objects. Support both.
    let trimmed = body.trim();
    if let Ok(ts_raw) = trimmed.parse::<i64>() {
        let secs = if ts_raw > 1_000_000_000_000 {
            ts_raw / 1000
        } else {
            ts_raw
        };
        return Ok(secs.to_string());
    }

    let json: serde_json::Value =
        serde_json::from_str(trimmed).context("failed to parse /time response")?;
    let ts_raw = if let Some(v) = json.as_i64() {
        v
    } else if let Some(s) = json.as_str() {
        s.parse::<i64>()
            .context("failed to parse string timestamp in /time response")?
    } else {
        json.get("epoch_ms")
            .or_else(|| json.get("epochMs"))
            .or_else(|| json.get("timestamp"))
            .or_else(|| json.get("time"))
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("missing timestamp field in /time response"))?
    };

    let secs = if ts_raw > 1_000_000_000_000 {
        ts_raw / 1000
    } else {
        ts_raw
    };
    Ok(secs.to_string())
}

fn sign_l1_auth(
    signer: &PrivateKeySigner,
    chain_id: u64,
    address: &str,
    timestamp: &str,
    nonce: u64,
) -> Result<String> {
    let address: Address = address.parse().context("invalid wallet address")?;
    let domain_separator = compute_domain_separator(chain_id);
    let struct_hash = compute_clob_auth_struct_hash(address, timestamp, nonce);
    let digest = eip712_digest(domain_separator, struct_hash);
    let sig = signer
        .sign_hash_sync(&digest)
        .context("failed to sign L1 auth payload")?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

fn build_l1_headers(
    address: &str,
    signature: &str,
    timestamp: &str,
    nonce: u64,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert("POLY_ADDRESS", HeaderValue::from_str(address)?);
    headers.insert("POLY_SIGNATURE", HeaderValue::from_str(signature)?);
    headers.insert("POLY_TIMESTAMP", HeaderValue::from_str(timestamp)?);
    headers.insert("POLY_NONCE", HeaderValue::from_str(&nonce.to_string())?);
    Ok(headers)
}

async fn request_api_credentials(
    rest_url: &str,
    method: Method,
    path: &str,
    headers: HeaderMap,
    proxy_url: Option<&str>,
) -> std::result::Result<ApiCredentials, ApiError> {
    let rest_url = rest_url.trim_end_matches('/');
    let url = format!("{rest_url}{path}");
    let client = build_http_client(proxy_url).map_err(|e| ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: format!("failed to build HTTP client: {e}"),
    })?;

    let req = client.request(method, &url).headers(headers);
    let resp = req.send().await.map_err(|e| ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: format!("request failed: {e}"),
    })?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(ApiError { status, body });
    }

    serde_json::from_str::<ApiCredentials>(&body).map_err(|e| ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: format!("invalid credentials response: {e}; body={body}"),
    })
}

fn compute_domain_separator(chain_id: u64) -> B256 {
    let typehash = keccak256(EIP712_DOMAIN_TYPEHASH);
    let name_hash = keccak256(CLOB_AUTH_DOMAIN_NAME.as_bytes());
    let version_hash = keccak256(CLOB_AUTH_DOMAIN_VERSION.as_bytes());

    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(typehash.as_slice());
    buf.extend_from_slice(name_hash.as_slice());
    buf.extend_from_slice(version_hash.as_slice());
    buf.extend_from_slice(&U256::from(chain_id).to_be_bytes::<32>());
    keccak256(&buf)
}

fn compute_clob_auth_struct_hash(address: Address, timestamp: &str, nonce: u64) -> B256 {
    let typehash = keccak256(CLOB_AUTH_TYPEHASH);
    let ts_hash = keccak256(timestamp.as_bytes());
    let msg_hash = keccak256(CLOB_AUTH_MESSAGE.as_bytes());

    let mut buf = Vec::with_capacity(160);
    buf.extend_from_slice(typehash.as_slice());
    buf.extend_from_slice(&pad_address(address));
    buf.extend_from_slice(ts_hash.as_slice());
    buf.extend_from_slice(&U256::from(nonce).to_be_bytes::<32>());
    buf.extend_from_slice(msg_hash.as_slice());
    keccak256(&buf)
}

fn eip712_digest(domain_separator: B256, struct_hash: B256) -> B256 {
    let mut buf = Vec::with_capacity(66);
    buf.extend_from_slice(&[0x19, 0x01]);
    buf.extend_from_slice(domain_separator.as_slice());
    buf.extend_from_slice(struct_hash.as_slice());
    keccak256(&buf)
}

fn pad_address(addr: Address) -> [u8; 32] {
    let mut padded = [0u8; 32];
    padded[12..32].copy_from_slice(addr.as_slice());
    padded
}
