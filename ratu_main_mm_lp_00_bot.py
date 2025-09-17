#!/usr/bin/env python3

# ratu_main_mm_lp_00_bot.py
# Step 0: Test Ed25519 key and FIX logon permissions on all 3 endpoints
# (Order Entry, Market Data, Drop Copy) by querying current message limit rate.

import os
from pathlib import Path

from binance_fix_connector.fix_connector import (
    create_order_entry_session,
    create_market_data_session,
    create_drop_copy_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key

# Credential and config
path = config_path = os.path.join(Path(__file__).parent.resolve(), "config.ini")
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)

# FIX endpoints
FIX_OE_URL = "tcp+tls://fix-oe.binance.com:9000"
FIX_MD_URL = "tcp+tls://fix-md.binance.com:9000"
FIX_DC_URL = "tcp+tls://fix-dc.binance.com:9000"

# Helper mappings for rendering
RESOLUTIONS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY"}
LIMIT_TYPES = {"1": "ORDER_LIMIT", "2": "MESSAGE_LIMIT", "3": "SUBSCRIPTION_LIMIT"}

def show_rendered_limit_session(client):
    """Show current LIMITS for the session."""
    responses = client.retrieve_messages_until(message_type="XLR")
    for msg in responses:
        if msg.message_type.decode("utf-8") == "XLR":
            limits = 0 if not msg.get(25003) else int(msg.get(25003).decode("utf-8"))
            client.logger.info("Parsing LimitResponse (XLR)")
            client.logger.info(f"Limits: ({limits})")
            for i in range(limits):
                limit_type = None if not msg.get(25004, i + 1) else msg.get(25004, i + 1).decode("utf-8")
                limit_count = None if not msg.get(25005, i + 1) else msg.get(25005, i + 1).decode("utf-8")
                limit_max = None if not msg.get(25006, i + 1) else msg.get(25006, i + 1).decode("utf-8")
                interval = None if not msg.get(25007, i + 1) else msg.get(25007, i + 1).decode("utf-8")
                interval_res = None if not msg.get(25008, i + 1) else msg.get(25008, i + 1).decode("utf-8")
                interval_str = "" if not interval else f"| Interval: {interval} {RESOLUTIONS.get(interval_res, interval_res)}"
                _info = f"Type: {LIMIT_TYPES.get(limit_type, limit_type)} | Count: {limit_count} | Max: {limit_max} {interval_str}"
                client.logger.info(_info)

# --- 1. Test Order Entry Session ---
client_oe = create_order_entry_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_OE_URL,
)
ack = client_oe.retrieve_messages_until(message_type="A")
if ack:
    client_oe.logger.info("Order Entry session logon OK — authentication and connection validated.")
    client_oe.logger.info("LimitQuery (XLQ) — OE")
    msg = client_oe.create_fix_message_with_basic_header("XLQ")
    msg.append_pair(6136, "current_message_rate")
    client_oe.send_message(msg)
    show_rendered_limit_session(client_oe)
else:
    client_oe.logger.error("Order Entry session logon failed — check credentials or permissions.")
client_oe.logger.info("LOGOUT (5)")
client_oe.logout()
client_oe.retrieve_messages_until(message_type="5")
client_oe.disconnect()

# --- 2. Test Market Data Session ---
client_md = create_market_data_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_MD_URL,
)
ack = client_md.retrieve_messages_until(message_type="A")
if ack:
    client_md.logger.info("Market Data session logon OK — authentication and connection validated.")
    client_md.logger.info("LimitQuery (XLQ) — MD")
    msg = client_md.create_fix_message_with_basic_header("XLQ")
    msg.append_pair(6136, "current_message_rate")
    client_md.send_message(msg)
    show_rendered_limit_session(client_md)
else:
    client_md.logger.error("Market Data session logon failed — check credentials or permissions.")
client_md.logger.info("LOGOUT (5)")
client_md.logout()
client_md.retrieve_messages_until(message_type="5")
client_md.disconnect()

# --- 3. Test Drop Copy Session (Logon/Logout only; XLQ not allowed) ---
client_dc = create_drop_copy_session(
    api_key=API_KEY,
    private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
    endpoint=FIX_DC_URL,
)
ack = client_dc.retrieve_messages_until(message_type="A")
if ack:
    client_dc.logger.info("Drop Copy session logon OK — authentication and connection validated.")
else:
    client_dc.logger.error("Drop Copy session logon failed — check credentials or permissions.")
client_dc.logger.info("LOGOUT (5)")
client_dc.logout()
client_dc.retrieve_messages_until(message_type="5")
client_dc.disconnect()
