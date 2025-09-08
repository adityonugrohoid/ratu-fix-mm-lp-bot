#!/usr/bin/env python3

import os
from pathlib import Path
from binance_fix_connector.fix_connector import (
    create_market_data_session,
    create_order_entry_session,
    create_drop_copy_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key

# ---- Config and Key Loading ----
CONFIG_PATH = os.path.join(Path(__file__).parent.resolve(), "config.ini")
API_KEY, PRIVATE_KEY_PATH = get_api_key(CONFIG_PATH)
PRIVATE_KEY = get_private_key(PRIVATE_KEY_PATH)

FIX_OE_URL = "tcp+tls://fix-oe.binance.com:9000"
FIX_MD_URL = "tcp+tls://fix-md.binance.com:9000"
FIX_DC_URL = "tcp+tls://fix-dc.binance.com:9000"

def test_fix_session(session_factory, name, **kwargs):
    print(f"\n--- Testing {name} session ---")
    try:
        client = session_factory(
            api_key=API_KEY,
            private_key=PRIVATE_KEY,
            endpoint=kwargs.get("endpoint"),
        )

        responses = client.retrieve_messages_until(message_type="A")
        if not responses:
            print(f"{name} logon: FAILED (no logon response)")
            client.disconnect()
            return False

        # Parse last message
        logon_msg = responses[-1]
        msg_type = logon_msg.get("35").decode("utf-8") if logon_msg.get("35") else None

        # Check for Reject ("3") or Logout ("5") immediately after logon
        # Also, look for error tags like 58 (Text), 373 (SessionRejectReason)
        if msg_type == "A":
            print(f"{name} logon: OK (received Logon message)")
            # Optional: check for suspicious '58' or error text in the logon message
        elif msg_type in ("3", "5"):
            error_text = logon_msg.get("58").decode("utf-8") if logon_msg.get("58") else "(no reason provided)"
            print(f"{name} logon: FAILED (server sent {'Reject' if msg_type=='3' else 'Logout'})")
            print(f"Reason: {error_text}")
            client.disconnect()
            return False
        else:
            print(f"{name} logon: FAILED (unexpected message type {msg_type})")
            client.disconnect()
            return False

        client.logout()
        client.retrieve_messages_until(message_type="5")
        client.disconnect()
        print(f"{name} logout/disconnect: OK\n")
        return True

    except Exception as e:
        print(f"Error in {name}: {e}")
        return False

if __name__ == "__main__":
    results = []
    results.append(test_fix_session(create_order_entry_session, "OrderEntry", endpoint=FIX_OE_URL))
    results.append(test_fix_session(create_market_data_session, "MarketData", endpoint=FIX_MD_URL))
    results.append(test_fix_session(create_drop_copy_session, "DropCopy", endpoint=FIX_DC_URL))

    if all(results):
        print("\nAll FIX connections (OrderEntry, MarketData, DropCopy): SUCCESS")
    else:
        failed = [name for i, name in enumerate(["OrderEntry", "MarketData", "DropCopy"]) if not results[i]]
        print("\nWARNING: The following FIX connections failed: " + ", ".join(failed))
