#!/usr/bin/env python3
# ratu_main_mm_lp_01_session.py
# Step 1: Baseline session setup — connect to OE and MD endpoints.

import os
from pathlib import Path
from binance_fix_connector.fix_connector import (
    create_order_entry_session,
    create_market_data_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key

# ---- Config and Key Loading ----
CONFIG_PATH = os.path.join(Path(__file__).parent.resolve(), "config.ini")
API_KEY, PRIVATE_KEY_PATH = get_api_key(CONFIG_PATH)
PRIVATE_KEY = get_private_key(PRIVATE_KEY_PATH)

FIX_OE_URL = "tcp+tls://fix-oe.binance.com:9000"
FIX_MD_URL = "tcp+tls://fix-md.binance.com:9000"

def connect_fix_sessions():
    print("\n--- Step 1: Session Setup ---")
    print("Connecting to FIX Order Entry session...")
    oe_client = create_order_entry_session(
        api_key=API_KEY,
        private_key=PRIVATE_KEY,
        endpoint=FIX_OE_URL,
    )
    print("Order Entry session: connected.")

    print("Connecting to FIX Market Data session...")
    md_client = create_market_data_session(
        api_key=API_KEY,
        private_key=PRIVATE_KEY,
        endpoint=FIX_MD_URL,
    )
    print("Market Data session: connected.")

    return oe_client, md_client

if __name__ == "__main__":
    oe_client, md_client = connect_fix_sessions()
    print("Both FIX sessions established. Ready for next step (MD subscribe, order logic, etc).")

    # For baseline, clean logout/disconnect (best practice for safe session close)
    oe_client.logout()
    oe_client.disconnect()
    md_client.logout()
    md_client.disconnect()
    print("Sessions closed. Step 1 baseline OK.\n")
