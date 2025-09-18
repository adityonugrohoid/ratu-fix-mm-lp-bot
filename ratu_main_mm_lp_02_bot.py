#!/usr/bin/env python3

# ratu_main_mm_lp_02_bot.py
# MM: persistent MD+OE sessions, pro event-driven double-quote logic, never miss fills

import time
import os
from pathlib import Path
from datetime import datetime

from binance_fix_connector.fix_connector import (
    create_order_entry_session,
    create_market_data_session,
)
from binance_fix_connector.utils import get_api_key, get_private_key

# --- Config & Endpoints ---
path = os.path.join(Path(__file__).parent.resolve(), "config.ini")
API_KEY, PATH_TO_PRIVATE_KEY_PEM_FILE = get_api_key(path)
FIX_MD_URL = "tcp+tls://fix-md.binance.com:9000"
FIX_OE_URL = "tcp+tls://fix-oe.binance.com:9000"
INSTRUMENT = "ETHFDUSD"
ORDER_QTY = 0.0011

ORD_STATUS = {"0": "NEW", "1": "PARTIALLY_FILLED", "2": "FILLED", "4": "CANCELED", "6": "PENDING_CANCEL", "8": "REJECTED", "A": "PENDING_NEW", "C": "EXPIRED"}
SIDES = {"1": "BUY", "2": "SELL"}

def get_best_bid_ask(md_client):
    """Fetch latest best bid/ask from most recent bookticker W snapshot."""
    while True:
        snapshot = md_client.retrieve_messages_until(message_type="W")
        bid, ask = None, None
        for msg in snapshot:
            if msg.message_type.decode("utf-8") == "W":
                n = int(msg.get(268).decode("utf-8"))
                for i in range(n):
                    typ = msg.get(269, i + 1).decode("utf-8")
                    px = msg.get(270, i + 1).decode("utf-8")
                    if typ == "0":
                        bid = px
                    elif typ == "1":
                        ask = px
        if bid and ask:
            return bid, ask
        time.sleep(0.05)

def place_limit_order(oe_client, side, price, qty, symbol, cl_prefix):
    cl_ord_id = f"{cl_prefix}{int(time.time_ns())}"
    msg = oe_client.create_fix_message_with_basic_header("D")
    msg.append_pair(38, qty)
    msg.append_pair(40, 2)  # LIMIT
    msg.append_pair(11, cl_ord_id)
    msg.append_pair(44, price)
    msg.append_pair(54, side)
    msg.append_pair(55, symbol)
    msg.append_pair(59, 1)  # GTC
    oe_client.send_message(msg)
    return cl_ord_id

def main():
    # --- 1. Create persistent sessions
    client_md = create_market_data_session(
        api_key=API_KEY,
        private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
        endpoint=FIX_MD_URL,
    )
    client_oe = create_order_entry_session(
        api_key=API_KEY,
        private_key=get_private_key(PATH_TO_PRIVATE_KEY_PEM_FILE),
        endpoint=FIX_OE_URL,
    )

    # --- 2. Logon handshake, optional one-time flush for true leftovers
    if not client_md.retrieve_messages_until(message_type="A"):
        print("MD logon failed."); return
    if not client_oe.retrieve_messages_until(message_type="A"):
        print("OE logon failed."); return
    client_oe.get_all_new_messages_received()  # Only ONCE, right after logon!

    # --- 3. Bookticker subscribe ONCE (persistent stream)
    msg = client_md.create_fix_message_with_basic_header("V")
    msg.append_pair(262, "BOOK_TICKER_STREAM")
    msg.append_pair(263, 1)  # Subscribe
    msg.append_pair(264, 1)
    msg.append_pair(266, "Y")
    msg.append_pair(146, 1)
    msg.append_pair(55, INSTRUMENT)
    msg.append_pair(267, 2)
    msg.append_pair(269, 0)
    msg.append_pair(269, 1)
    client_md.logger.info(f"Persistent subscribing to {INSTRUMENT} book ticker")
    client_md.send_message(msg)

    # --- 4. MM cycle: Place double-quote and listen for fills (event-driven)
    fill_timeout_sec = 120
    open_orders = {}  # cl_ord_id: {'side': .., 'status': ..}

    try:
        # --- Only one cycle for demo, ready for loop/infinite expansion
        bid, ask = get_best_bid_ask(client_md)
        client_md.logger.info(f"Best bid: {bid}, best ask: {ask}")

        # Place double quote orders and add to watchlist
        buy_id = place_limit_order(client_oe, 1, bid, ORDER_QTY, INSTRUMENT, "B")
        sell_id = place_limit_order(client_oe, 2, ask, ORDER_QTY, INSTRUMENT, "S")
        open_orders[buy_id] = {'side': 'BUY', 'status': None, 'filled': False}
        open_orders[sell_id] = {'side': 'SELL', 'status': None, 'filled': False}
        client_oe.logger.info(f"Placed BUY@{bid} (ID {buy_id}), SELL@{ask} (ID {sell_id})")

        # --- 5. Unified fill event listening loop
        deadline = time.time() + fill_timeout_sec
        while time.time() < deadline and (not open_orders[buy_id]['filled'] or not open_orders[sell_id]['filled']):
            reports = client_oe.retrieve_messages_until(message_type="8")
            for resp in reports:
                cl_ord_id = resp.get(11).decode("utf-8") if resp.get(11) else None
                ord_status = resp.get(39).decode("utf-8") if resp.get(39) else None
                cum_qty = resp.get(14).decode("utf-8") if resp.get(14) else None
                if cl_ord_id in open_orders:
                    open_orders[cl_ord_id]['status'] = ORD_STATUS.get(ord_status, ord_status)
                    if ord_status == "2":  # FILLED
                        open_orders[cl_ord_id]['filled'] = True
                        client_oe.logger.info(f"[FILL CHECK] {open_orders[cl_ord_id]['side']} order {cl_ord_id}: FILLED (CumQty={cum_qty})")
                    else:
                        client_oe.logger.info(f"[FILL CHECK] {open_orders[cl_ord_id]['side']} order {cl_ord_id}: Status={ORD_STATUS.get(ord_status,ord_status)} (CumQty={cum_qty})")
            if open_orders[buy_id]['filled'] and open_orders[sell_id]['filled']:
                client_oe.logger.info("Both orders FILLED — MM cycle complete")
                break
            time.sleep(1)

        # --- Timeout? Log any not-filled orders
        for cl_ord_id, info in open_orders.items():
            if not info['filled']:
                client_oe.logger.warning(f"{info['side']} order {cl_ord_id} NOT FILLED by timeout")

    finally:
        # --- Graceful MD Unsubscribe & Logout ---
        unsub = client_md.create_fix_message_with_basic_header("V")
        unsub.append_pair(262, "BOOK_TICKER_STREAM")
        unsub.append_pair(263, 2)
        unsub.append_pair(264, 1)
        unsub.append_pair(266, "Y")
        unsub.append_pair(146, 1)
        unsub.append_pair(55, INSTRUMENT)
        unsub.append_pair(267, 1)
        unsub.append_pair(269, 2)
        client_md.send_message(unsub)
        client_md.logger.info("Unsubscribed from MD stream.")

        client_md.logger.info("MD LOGOUT (5)")
        client_md.logout()
        client_md.retrieve_messages_until(message_type="5")
        client_md.logger.info("MD session closed.")
        client_md.disconnect()

        client_oe.logger.info("OE LOGOUT (5)")
        client_oe.logout()
        client_oe.retrieve_messages_until(message_type="5")
        client_oe.logger.info("OE session closed.")
        client_oe.disconnect()

if __name__ == "__main__":
    main()
