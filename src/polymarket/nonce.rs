use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::U256;

// ---------------------------------------------------------------------------
// Nonce Manager
// ---------------------------------------------------------------------------

/// Thread-safe, monotonically increasing nonce tracker for order signing.
///
/// Polymarket requires each signed order to have a unique nonce.
/// This manager provides atomic incrementing to ensure uniqueness
/// even under concurrent order submission.
pub struct NonceManager {
    current: AtomicU64,
}

impl NonceManager {
    /// Create a new nonce manager starting from 0.
    pub fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
        }
    }

    /// Create a nonce manager starting from a specific value.
    /// Useful for resuming from a known last-used nonce.
    pub fn from_value(start: u64) -> Self {
        Self {
            current: AtomicU64::new(start),
        }
    }

    /// Get the next nonce and atomically increment the counter.
    pub fn next(&self) -> U256 {
        let val = self.current.fetch_add(1, Ordering::SeqCst);
        U256::from(val)
    }

    /// Peek at the current nonce value without incrementing.
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }
}

impl Default for NonceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonce_increments() {
        let nm = NonceManager::new();
        assert_eq!(nm.next(), U256::from(0));
        assert_eq!(nm.next(), U256::from(1));
        assert_eq!(nm.next(), U256::from(2));
        assert_eq!(nm.current(), 3);
    }

    #[test]
    fn nonce_from_value() {
        let nm = NonceManager::from_value(100);
        assert_eq!(nm.next(), U256::from(100));
        assert_eq!(nm.next(), U256::from(101));
    }
}
