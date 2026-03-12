"""
Derive Polymarket CLOB L2 API credentials from an L1 private key.

Requirements:
  pip install py-clob-client

Usage (PowerShell):
  $env:PRIVATE_KEY="0x...."
  python .\scripts\get_polymarket_api_creds.py
"""

from __future__ import annotations

import os
import sys

from py_clob_client.client import ClobClient


def main() -> int:
    private_key = os.getenv("PRIVATE_KEY")
    if not private_key:
        print("ERROR: PRIVATE_KEY env var is missing.", file=sys.stderr)
        print('Set it first, e.g. $env:PRIVATE_KEY="0x..."', file=sys.stderr)
        return 1

    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,  # Polygon mainnet
        key=private_key,
    )

    creds = client.create_or_derive_api_creds()

    # Compatible with multiple py-clob-client return shapes.
    api_key = getattr(creds, "api_key", None) or creds.get("apiKey")
    secret = getattr(creds, "api_secret", None) or creds.get("secret")
    passphrase = getattr(creds, "api_passphrase", None) or creds.get("passphrase")

    print("AXELBOT_API_KEY=" + str(api_key))
    print("AXELBOT_API_SECRET=" + str(secret))
    print("AXELBOT_API_PASSPHRASE=" + str(passphrase))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
