use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use tokio_tungstenite::MaybeTlsStream;

/// Build a `reqwest::Client` that optionally routes all traffic through a proxy.
///
/// Supports HTTP(S) and SOCKS5 proxy URLs (e.g. `socks5://host:port`,
/// `http://proxy:8080`). When `proxy_url` is `None` or empty the client
/// behaves identically to `reqwest::Client::new()`.
pub fn build_http_client(proxy_url: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder();
    if let Some(url) = proxy_url {
        let url = url.trim();
        if !url.is_empty() {
            builder = builder.proxy(reqwest::Proxy::all(url).context("invalid proxy URL")?);
        } else {
            // Keep startup deterministic when proxy isn't explicitly configured.
            // Without this, reqwest may silently pick ambient *_PROXY env vars.
            builder = builder.no_proxy();
        }
    } else {
        // Keep startup deterministic when proxy isn't explicitly configured.
        // Without this, reqwest may silently pick ambient *_PROXY env vars.
        builder = builder.no_proxy();
    }
    builder.build().context("failed to build HTTP client")
}

/// Connect a WebSocket through an optional SOCKS5/HTTP proxy.
///
/// When no proxy is configured this is equivalent to a plain
/// `tokio_tungstenite::connect_async`.  For SOCKS5 proxies we use
/// `tokio-socks` to establish the TCP tunnel first, then upgrade to TLS+WS.
pub async fn connect_ws_proxied(
    url: &str,
    proxy_url: Option<&str>,
) -> Result<(
    tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
)> {
    let proxy = proxy_url.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    if let Some(proxy) = proxy {
        match connect_ws_via_proxy(url, &proxy).await {
            Ok(v) => Ok(v),
            Err(err) => {
                eprintln!(
                    "warning: WS proxy connection failed for {:?}: {}. falling back to direct WS",
                    proxy, err
                );
                let (ws, resp) = tokio_tungstenite::connect_async(url)
                    .await
                    .context("WebSocket connect failed after proxy fallback")?;
                Ok((ws, resp))
            }
        }
    } else {
        let (ws, resp) = tokio_tungstenite::connect_async(url)
            .await
            .context("WebSocket connect failed")?;
        Ok((ws, resp))
    }
}

async fn connect_ws_via_proxy(
    ws_url: &str,
    proxy_url: &str,
) -> Result<(
    tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
)> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    // Parse the WS URL to extract host:port for the proxy destination
    let parsed: url::Url = ws_url.parse().context("invalid WebSocket URL")?;
    let ws_host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("WebSocket URL has no host"))?;
    let ws_port = parsed.port_or_known_default().unwrap_or(443);

    // Parse proxy URL
    let proxy_parsed: url::Url = proxy_url.parse().context("invalid proxy URL")?;
    let proxy_host = proxy_parsed
        .host_str()
        .ok_or_else(|| anyhow!("proxy URL has no host"))?;
    let proxy_port = proxy_parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("proxy URL has no port"))?;
    let proxy_addr = format!("{}:{}", proxy_host, proxy_port);

    let scheme = proxy_parsed.scheme().to_ascii_lowercase();

    // Establish a raw TCP stream through the proxy
    let tcp_stream: tokio::net::TcpStream = match scheme.as_str() {
        "socks5" | "socks5h" => {
            let username = proxy_parsed.username();
            let password = proxy_parsed.password().unwrap_or("");
            let target = (ws_host.to_string(), ws_port);

            if !username.is_empty() {
                tokio_socks::tcp::Socks5Stream::connect_with_password(
                    proxy_addr.as_str(),
                    target,
                    username,
                    password,
                )
                .await
                .context("SOCKS5 auth connect failed")?
                .into_inner()
            } else {
                tokio_socks::tcp::Socks5Stream::connect(proxy_addr.as_str(), target)
                    .await
                    .context("SOCKS5 connect failed")?
                    .into_inner()
            }
        }
        "http" | "https" => {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut tcp = tokio::net::TcpStream::connect(&proxy_addr)
                .await
                .context("failed to connect to HTTP proxy")?;
            let connect_req = format!(
                "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
                ws_host, ws_port, ws_host, ws_port
            );
            tcp.write_all(connect_req.as_bytes()).await?;
            let mut buf = [0u8; 1024];
            let n = tcp.read(&mut buf).await?;
            let resp = String::from_utf8_lossy(&buf[..n]);
            if !resp.contains("200") {
                return Err(anyhow!("HTTP CONNECT tunnel failed: {}", resp.trim()));
            }
            tcp
        }
        _ => {
            return Err(anyhow!(
                "unsupported proxy scheme '{}'; use socks5:// or http://",
                scheme
            ));
        }
    };

    // Upgrade the tunneled TCP stream to TLS + WebSocket.
    // `client_async_tls_with_config` with `rustls-tls-native-roots` feature
    // handles wss:// TLS negotiation and returns WebSocketStream<MaybeTlsStream<TcpStream>>.
    let request = ws_url.into_client_request().context("bad WS request")?;
    let (ws_stream, response) =
        tokio_tungstenite::client_async_tls_with_config(request, tcp_stream, None, None)
            .await
            .context("WebSocket TLS handshake through proxy failed")?;

    Ok((ws_stream, response))
}
