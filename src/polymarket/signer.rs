use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};

use super::types::{ClobSide, SignedClobOrder};

// ---------------------------------------------------------------------------
// EIP-712 Domain & Type Hashes (Polymarket CTF Exchange)
// ---------------------------------------------------------------------------

/// EIP-712 domain separator components
const EIP712_DOMAIN_TYPEHASH: &[u8] =
    b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
const PROTOCOL_NAME: &str = "Polymarket CTF Exchange";
const PROTOCOL_VERSION: &str = "1";

/// Order struct typehash for EIP-712
const ORDER_TYPEHASH: &[u8] = b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";

/// Zero address used as taker for open orders
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Signature type for EOA wallets
const SIGNATURE_TYPE_EOA: u8 = 0;

// ---------------------------------------------------------------------------
// Order parameters (input to signing)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OrderParams {
    pub token_id: String,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub side: ClobSide,
    pub fee_rate_bps: u64,
    pub nonce: U256,
    pub expiration: U256,
}

// ---------------------------------------------------------------------------
// Signer
// ---------------------------------------------------------------------------

pub struct PolymarketSigner {
    wallet: PrivateKeySigner,
    maker_address: Address,
    chain_id: u64,
    ctf_exchange: Address,
    neg_risk_ctf_exchange: Address,
    domain_separator: B256,
    neg_risk_domain_separator: B256,
}

impl PolymarketSigner {
    /// Create a new signer from a hex-encoded private key.
    pub fn new(
        private_key_hex: &str,
        chain_id: u64,
        ctf_exchange_address: &str,
        neg_risk_ctf_exchange_address: &str,
    ) -> Result<Self> {
        let key_hex = private_key_hex.strip_prefix("0x").unwrap_or(private_key_hex);
        let wallet: PrivateKeySigner = key_hex
            .parse()
            .context("failed to parse private key")?;

        let maker_address = wallet.address();
        let ctf_exchange: Address = ctf_exchange_address
            .parse()
            .context("invalid CTF exchange address")?;
        let neg_risk_ctf_exchange: Address = neg_risk_ctf_exchange_address
            .parse()
            .context("invalid NegRisk CTF exchange address")?;

        let domain_separator = compute_domain_separator(chain_id, ctf_exchange);
        let neg_risk_domain_separator = compute_domain_separator(chain_id, neg_risk_ctf_exchange);

        Ok(Self {
            wallet,
            maker_address,
            chain_id,
            ctf_exchange,
            neg_risk_ctf_exchange,
            domain_separator,
            neg_risk_domain_separator,
        })
    }

    pub fn maker_address(&self) -> Address {
        self.maker_address
    }

    /// Sign an order and return the full `SignedClobOrder` ready for POST /order.
    pub fn sign_order(&self, params: &OrderParams, neg_risk: bool) -> Result<SignedClobOrder> {
        let salt = U256::from(rand::random::<u128>());

        let side_byte: u8 = match params.side {
            ClobSide::Buy => 0,
            ClobSide::Sell => 1,
        };

        let token_id = U256::from_str_radix(
            params.token_id.strip_prefix("0x").unwrap_or(&params.token_id),
            if params.token_id.starts_with("0x") { 16 } else { 10 },
        )
        .context("invalid token_id")?;

        let struct_hash = compute_order_struct_hash(
            salt,
            self.maker_address,
            self.maker_address, // signer == maker for EOA
            ZERO_ADDRESS.parse::<Address>().unwrap(),
            token_id,
            params.maker_amount,
            params.taker_amount,
            params.expiration,
            params.nonce,
            U256::from(params.fee_rate_bps),
            side_byte,
            SIGNATURE_TYPE_EOA,
        );

        let domain_sep = if neg_risk {
            self.neg_risk_domain_separator
        } else {
            self.domain_separator
        };

        let digest = eip712_digest(domain_sep, struct_hash);

        let signature = self
            .wallet
            .sign_hash_sync(&digest)
            .context("failed to sign order hash")?;
        let sig_bytes = hex::encode(signature.as_bytes());

        Ok(SignedClobOrder {
            salt: salt.to_string(),
            maker: format!("{:?}", self.maker_address),
            signer: format!("{:?}", self.maker_address),
            taker: ZERO_ADDRESS.to_string(),
            token_id: params.token_id.clone(),
            maker_amount: params.maker_amount.to_string(),
            taker_amount: params.taker_amount.to_string(),
            expiration: params.expiration.to_string(),
            nonce: params.nonce.to_string(),
            fee_rate_bps: params.fee_rate_bps.to_string(),
            side: side_byte.to_string(),
            signature_type: SIGNATURE_TYPE_EOA.to_string(),
            signature: format!("0x{sig_bytes}"),
        })
    }
}

// ---------------------------------------------------------------------------
// EIP-712 hashing helpers
// ---------------------------------------------------------------------------

fn compute_domain_separator(chain_id: u64, verifying_contract: Address) -> B256 {
    let typehash = keccak256(EIP712_DOMAIN_TYPEHASH);
    let name_hash = keccak256(PROTOCOL_NAME.as_bytes());
    let version_hash = keccak256(PROTOCOL_VERSION.as_bytes());

    let mut buf = Vec::with_capacity(160);
    buf.extend_from_slice(typehash.as_slice());
    buf.extend_from_slice(name_hash.as_slice());
    buf.extend_from_slice(version_hash.as_slice());
    buf.extend_from_slice(&U256::from(chain_id).to_be_bytes::<32>());
    buf.extend_from_slice(&pad_address(verifying_contract));

    keccak256(&buf)
}

#[allow(clippy::too_many_arguments)]
fn compute_order_struct_hash(
    salt: U256,
    maker: Address,
    signer: Address,
    taker: Address,
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    expiration: U256,
    nonce: U256,
    fee_rate_bps: U256,
    side: u8,
    signature_type: u8,
) -> B256 {
    let typehash = keccak256(ORDER_TYPEHASH);

    let mut buf = Vec::with_capacity(13 * 32);
    buf.extend_from_slice(typehash.as_slice());
    buf.extend_from_slice(&salt.to_be_bytes::<32>());
    buf.extend_from_slice(&pad_address(maker));
    buf.extend_from_slice(&pad_address(signer));
    buf.extend_from_slice(&pad_address(taker));
    buf.extend_from_slice(&token_id.to_be_bytes::<32>());
    buf.extend_from_slice(&maker_amount.to_be_bytes::<32>());
    buf.extend_from_slice(&taker_amount.to_be_bytes::<32>());
    buf.extend_from_slice(&expiration.to_be_bytes::<32>());
    buf.extend_from_slice(&nonce.to_be_bytes::<32>());
    buf.extend_from_slice(&fee_rate_bps.to_be_bytes::<32>());
    buf.extend_from_slice(&U256::from(side).to_be_bytes::<32>());
    buf.extend_from_slice(&U256::from(signature_type).to_be_bytes::<32>());

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: create a signer and sign an order without panicking.
    #[test]
    fn test_sign_order_smoke() {
        // Use a well-known test private key (DO NOT use in production)
        let test_key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let ctf = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
        let neg_risk_ctf = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

        let signer =
            PolymarketSigner::new(test_key, 137, ctf, neg_risk_ctf).expect("signer creation");

        let params = OrderParams {
            token_id: "12345".to_string(),
            maker_amount: U256::from(100_000_000u64), // 100 USDC (6 decimals)
            taker_amount: U256::from(200_000_000u64),
            side: ClobSide::Buy,
            fee_rate_bps: 200,
            nonce: U256::from(0u64),
            expiration: U256::from(0u64), // no expiration
        };

        let signed = signer.sign_order(&params, false).expect("signing");
        assert!(signed.signature.starts_with("0x"));
        assert!(!signed.salt.is_empty());
        assert_eq!(signed.side, "0");
        assert_eq!(signed.signature_type, "0");
    }

    #[test]
    fn test_domain_separator_deterministic() {
        let ctf: Address = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
            .parse()
            .unwrap();
        let sep1 = compute_domain_separator(137, ctf);
        let sep2 = compute_domain_separator(137, ctf);
        assert_eq!(sep1, sep2);
    }

    #[test]
    fn test_neg_risk_uses_different_domain() {
        let test_key = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let ctf = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
        let neg_risk_ctf = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

        let signer = PolymarketSigner::new(test_key, 137, ctf, neg_risk_ctf).unwrap();
        assert_ne!(signer.domain_separator, signer.neg_risk_domain_separator);
    }
}
