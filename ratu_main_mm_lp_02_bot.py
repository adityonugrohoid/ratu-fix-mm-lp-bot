#!/usr/bin/env python3
# ratu_main_mm_lp_01_bot.py
"""
Modular Spread Market-Making bot using Binance FIX API (Market Data & Order Entry).
"""
import time
from datetime import datetime, timedelta
from configparser import ConfigParser

from binance_fix_connector.fix_connector import create_market_data_session, create_order_entry_session
from binance_fix_connector.utils import get_api_key, get_private_key

# --- begin: errors-only logging helper ---
import logging

def _setup_logging_min_level(level_name: str = "ERROR") -> None:
    lvl = getattr(logging, level_name.upper(), logging.ERROR)
    # Root & common libraries
    logging.getLogger().setLevel(lvl)
    for name in ("binance_fix_connector", "simplefix", "urllib3", "asyncio"):
        logging.getLogger(name).setLevel(lvl)

    # Force handlers (if any) to the same level
    root = logging.getLogger()
    if not root.handlers:
        # minimal console handler if none exists
        h = logging.StreamHandler()
        h.setLevel(lvl)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        h.setFormatter(fmt)
        root.addHandler(h)
    else:
        for h in root.handlers:
            h.setLevel(lvl)
# --- end: errors-only logging helper ---

# FIX enumerations (for logging and interpretation)
UPDATE = {"0": "BID", "1": "OFFER", "2": "TRADE"}
ACTION = {"0": "NEW", "1": "CHANGE", "2": "DELETE"}
AGGRESSOR_SIDE = {"1": "BUY", "2": "SELL"}
ORD_STATUS = {
    "0": "NEW",
    "1": "PARTIALLY_FILLED",
    "2": "FILLED",
    "4": "CANCELED",
    "6": "PENDING_CANCEL",
    "8": "REJECTED",
    "A": "PENDING_NEW",
    "C": "EXPIRED",
}
ORD_TYPES = {"1": "MARKET", "2": "LIMIT", "3": "STOP", "4": "STOP_LIMIT"}
SIDES = {"1": "BUY", "2": "SELL"}
TIME_IN_FORCE = {
    "1": "GOOD_TILL_CANCEL",
    "3": "IMMEDIATE_OR_CANCEL",
    "4": "FILL_OR_KILL",
}
ORD_REJECT_REASON = {"99": "OTHER"}
RESOLUTIONS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY"}
LIMIT_TYPES = {"1": "ORDER_LIMIT", "2": "MESSAGE_LIMIT", "3": "SUBSCRIPTION_LIMIT"}

def handle_quote_decision(state):
    """
    Determine quoting actions (e.g., placing/cancelling orders) based on current market state.
    state: dict with keys ['best_bid', 'best_ask', 'order_book', 'book_updates', 'recent_trades'].
    Returns a list of intent dicts, e.g., {"type": "place_order", "side": "BUY", "price": ..., "qty": ..., "symbol": ..., "order_type": "LIMIT", "time_in_force": "GTC"}.
    """
    best_bid = state.get("best_bid")
    best_ask = state.get("best_ask")
    if not best_bid or not best_ask:
        return []  # not enough info to decide
    # Simple strategy: place one buy at current best bid and one sell at current best ask (spread capturing).
    symbol = state.get("symbol", "")
    # Use a small quantity for demonstration
    qty = 0.001
    # Price as float (from state)
    buy_price = best_bid
    sell_price = best_ask
    intents = []
    # Place a buy order at best bid
    intents.append({
        "type": "place_order",
        "side": "BUY",
        "price": buy_price,
        "qty": qty,
        "symbol": symbol,
        "order_type": "LIMIT",
        "time_in_force": "GTC",
    })
    # Place a sell order at best ask
    intents.append({
        "type": "place_order",
        "side": "SELL",
        "price": sell_price,
        "qty": qty,
        "symbol": symbol,
        "order_type": "LIMIT",
        "time_in_force": "GTC",
    })
    return intents

if __name__ == "__main__":
    # Load API credentials from config.ini
    config_path = "config.ini"
    API_KEY, PRIVATE_KEY_PATH = get_api_key(config_path)
    private_key = get_private_key(PRIVATE_KEY_PATH)
    # Load runtime settings from ratu_mm_settings.ini (with defaults)
    runtime_cfg_path = "ratu_mm_settings.ini"
    cfg = ConfigParser()
    cfg.read(runtime_cfg_path)

    log_level = cfg.get("runtime", "LOG_LEVEL", fallback="ERROR")
    _setup_logging_min_level(log_level)

    # Default runtime values
    symbol_list = cfg.get("runtime", "SYMBOLS", fallback="ETHFDUSD")
    streams_list = cfg.get("runtime", "STREAMS", fallback="book_ticker,depth,trades")
    market_depth = cfg.getint("runtime", "MARKET_DEPTH", fallback=50)
    recv_window_ms = cfg.getint("runtime", "RECV_WINDOW_MS", fallback=5000)
    enable_trading = cfg.getboolean("runtime", "ENABLE_TRADING", fallback=False)
    md_endpoint = cfg.get("runtime", "MD_ENDPOINT", fallback=None)
    oe_endpoint = cfg.get("runtime", "OE_ENDPOINT", fallback=None)
    # Prepare symbol(s) and streams
    symbols = [s.strip() for s in symbol_list.split(",") if s.strip()]
    if not symbols:
        raise ValueError("No SYMBOLS specified in config.")
    symbol = symbols[0]
    streams = [s.strip() for s in streams_list.split(",") if s.strip()]
    # Create FIX sessions for Market Data and Order Entry
    if oe_endpoint:
        client_oe = create_order_entry_session(api_key=API_KEY, private_key=private_key, endpoint=oe_endpoint)
    else:
        client_oe = create_order_entry_session(api_key=API_KEY, private_key=private_key)
    client_oe.retrieve_messages_until(message_type="A")
    if md_endpoint:
        client_md = create_market_data_session(api_key=API_KEY, private_key=private_key, endpoint=md_endpoint, recv_window=recv_window_ms)
    else:
        client_md = create_market_data_session(api_key=API_KEY, private_key=private_key, recv_window=recv_window_ms)
    client_md.retrieve_messages_until(message_type="A")

    for lg in (client_md.logger, client_oe.logger):
        try:
            lg.setLevel(getattr(logging, log_level.upper(), logging.ERROR))
            for h in lg.handlers:
                h.setLevel(getattr(logging, log_level.upper(), logging.ERROR))
        except Exception:
            # ignore if logger layout differs
            pass

    # Log initial configuration
    client_md.logger.info(f"Initialized Market Data & Order Entry sessions. Symbol={symbol}, Streams={streams}, ENABLE_TRADING={enable_trading}")
    # Perform LimitQuery (XLQ) on both sessions to get current limits
    # Order Entry session limit query
    msg_oe_lq = client_oe.create_fix_message_with_basic_header("XLQ")
    msg_oe_lq.append_pair(6136, "current_message_rate")
    client_oe.logger.info("LimitQuery (XLQ) on Order Entry session")
    client_oe.send_message(msg_oe_lq)
    responses = client_oe.retrieve_messages_until(message_type="XLR")
    for msg in responses:
        if msg.get("35").decode("utf-8") == "XLR":
            limits = 0 if not msg.get(25003) else int(msg.get(25003).decode("utf-8"))
            client_oe.logger.info("Parsing LimitResponse (XLR)")
            client_oe.logger.info(f"Limits: ({limits})")
            for i in range(limits):
                limit_type = None if not msg.get(25004, i+1) else msg.get(25004, i+1).decode("utf-8")
                limit_count = None if not msg.get(25005, i+1) else msg.get(25005, i+1).decode("utf-8")
                limit_max = None if not msg.get(25006, i+1) else msg.get(25006, i+1).decode("utf-8")
                interval = None if not msg.get(25007, i+1) else msg.get(25007, i+1).decode("utf-8")
                interval_res = None if not msg.get(25008, i+1) else msg.get(25008, i+1).decode("utf-8")
                interval_str = "" if not interval else f"| Interval: {interval} {RESOLUTIONS.get(interval_res, interval_res)}"
                info = f"Type: {LIMIT_TYPES.get(limit_type, limit_type)} | Count: {limit_count} | Max: {limit_max} {interval_str}"
                client_oe.logger.info(info)
    # Market Data session limit query
    msg_md_lq = client_md.create_fix_message_with_basic_header("XLQ")
    msg_md_lq.append_pair(6136, "current_message_rate")
    client_md.logger.info("LimitQuery (XLQ) on Market Data session")
    client_md.send_message(msg_md_lq)
    responses = client_md.retrieve_messages_until(message_type="XLR")
    for msg in responses:
        if msg.get("35").decode("utf-8") == "XLR":
            limits = 0 if not msg.get(25003) else int(msg.get(25003).decode("utf-8"))
            client_md.logger.info("Parsing LimitResponse (XLR)")
            client_md.logger.info(f"Limits: ({limits})")
            for i in range(limits):
                limit_type = None if not msg.get(25004, i+1) else msg.get(25004, i+1).decode("utf-8")
                limit_count = None if not msg.get(25005, i+1) else msg.get(25005, i+1).decode("utf-8")
                limit_max = None if not msg.get(25006, i+1) else msg.get(25006, i+1).decode("utf-8")
                interval = None if not msg.get(25007, i+1) else msg.get(25007, i+1).decode("utf-8")
                interval_res = None if not msg.get(25008, i+1) else msg.get(25008, i+1).decode("utf-8")
                interval_str = "" if not interval else f"| Interval: {interval} {RESOLUTIONS.get(interval_res, interval_res)}"
                info = f"Type: {LIMIT_TYPES.get(limit_type, limit_type)} | Count: {limit_count} | Max: {limit_max} {interval_str}"
                client_md.logger.info(info)
    # Subscribe to each requested market data stream (Market Data Request V)
    active_md_reqs = []
    state = None
    for stream in streams:
        stream_name = stream.lower()
        if stream_name == "depth":
            md_req_id = "DEPTH_STREAM"
            msg = client_md.create_fix_message_with_basic_header("V")
            msg.append_pair(262, md_req_id)
            msg.append_pair(263, 1)  # Subscribe
            msg.append_pair(264, market_depth)
            msg.append_pair(266, "Y")
            msg.append_pair(146, 1)
            msg.append_pair(55, symbol)
            msg.append_pair(267, 2)
            msg.append_pair(269, 0)
            msg.append_pair(269, 1)
            client_md.logger.info("*" * 50)
            client_md.logger.info("MARKET_DATA_REQUEST (V): SUBSCRIBING")
            client_md.logger.info("*" * 50)
            client_md.send_message(msg)
            client_md.logger.info(f"Subscribed to Depth stream for {symbol}, depth={market_depth}")
            # Retrieve and parse initial snapshot (W) for depth
            responses = client_md.retrieve_messages_until(message_type="W")
            for m in responses:
                if m.get("35").decode("utf-8") == "W":
                    client_md.logger.info("Parsing MarketDataSnapshot (W) for depth...")
                    sub_id = None if not m.get(262) else m.get(262).decode("utf-8")
                    updates = 0 if not m.get(268) else int(m.get(268).decode("utf-8"))
                    sym = None if not m.get(55) else m.get(55).decode("utf-8")
                    last_book_id = None if not m.get(25044) else m.get(25044).decode("utf-8")
                    header = f"Snapshot: {sub_id} -> {updates} updates for Symbol: {sym} LastBookId: {last_book_id}"
                    client_md.logger.info(header)
                    # Initialize order book state from snapshot
                    bids = []
                    asks = []
                    for i in range(updates):
                        update_type = None if not m.get(269, i+1) else m.get(269, i+1).decode("utf-8")
                        price = None if not m.get(270, i+1) else m.get(270, i+1).decode("utf-8")
                        qty = None if not m.get(271, i+1) else m.get(271, i+1).decode("utf-8")
                        client_md.logger.info(f"Update type: {UPDATE.get(update_type, update_type)} | Price: {price} | Qty: {qty}")
                        if update_type == "0" and price:
                            bids.append((float(price), float(qty) if qty else 0.0))
                        elif update_type == "1" and price:
                            asks.append((float(price), float(qty) if qty else 0.0))
                    bids.sort(key=lambda x: x[0], reverse=True)
                    asks.sort(key=lambda x: x[0])
                    state = {
                        "symbol": sym,
                        "best_bid": bids[0][0] if bids else None,
                        "best_ask": asks[0][0] if asks else None,
                        "order_book": {"bids": bids, "asks": asks},
                        "book_updates": [],      # incremental book changes will be stored
                        "recent_trades": [],      # trade events will be stored
                    }
            active_md_reqs.append((md_req_id, "depth"))
        elif stream_name == "book_ticker":
            md_req_id = "BOOK_TICKER_STREAM"
            msg = client_md.create_fix_message_with_basic_header("V")
            msg.append_pair(262, md_req_id)
            msg.append_pair(263, 1)
            msg.append_pair(264, 1)
            msg.append_pair(266, "Y")
            msg.append_pair(146, 1)
            msg.append_pair(55, symbol)
            msg.append_pair(267, 2)
            msg.append_pair(269, 0)
            msg.append_pair(269, 1)
            client_md.logger.info("*" * 50)
            client_md.logger.info("MARKET_DATA_REQUEST (V): SUBSCRIBING")
            client_md.logger.info("*" * 50)
            client_md.send_message(msg)
            client_md.logger.info(f"Subscribed to Book Ticker stream for {symbol}")
            responses = client_md.retrieve_messages_until(message_type="W")
            for m in responses:
                if m.get("35").decode("utf-8") == "W":
                    client_md.logger.info("Parsing MarketDataSnapshot (W) for book ticker...")
                    sub_id = None if not m.get(262) else m.get(262).decode("utf-8")
                    updates = 0 if not m.get(268) else int(m.get(268).decode("utf-8"))
                    sym = None if not m.get(55) else m.get(55).decode("utf-8")
                    last_book_id = None if not m.get(25044) else m.get(25044).decode("utf-8")
                    header = f"Snapshot: {sub_id} -> {updates} updates for Symbol: {sym} LastBookId: {last_book_id}"
                    client_md.logger.info(header)
                    # Parse snapshot updates (best bid and best ask)
                    for i in range(updates):
                        update_type = None if not m.get(269, i+1) else m.get(269, i+1).decode("utf-8")
                        price = None if not m.get(270, i+1) else m.get(270, i+1).decode("utf-8")
                        qty = None if not m.get(271, i+1) else m.get(271, i+1).decode("utf-8")
                        client_md.logger.info(f"Update type: {UPDATE.get(update_type, update_type)} | Price: {price} | Qty: {qty}")
                        if update_type == "0" and price:
                            if state:
                                state["best_bid"] = float(price)
                            else:
                                state = {
                                    "symbol": sym,
                                    "best_bid": float(price),
                                    "best_ask": None,
                                    "order_book": {"bids": [], "asks": []},
                                    "book_updates": [],
                                    "recent_trades": [],
                                }
                        if update_type == "1" and price:
                            if state:
                                state["best_ask"] = float(price)
                            else:
                                state = {
                                    "symbol": sym,
                                    "best_bid": None,
                                    "best_ask": float(price),
                                    "order_book": {"bids": [], "asks": []},
                                    "book_updates": [],
                                    "recent_trades": [],
                                }
            active_md_reqs.append((md_req_id, "book_ticker"))
        elif stream_name == "trades":
            md_req_id = "TRADE_STREAM"
            msg = client_md.create_fix_message_with_basic_header("V")
            msg.append_pair(262, md_req_id)
            msg.append_pair(263, 1)
            msg.append_pair(264, 1)
            msg.append_pair(266, "Y")
            msg.append_pair(146, 1)
            msg.append_pair(55, symbol)
            msg.append_pair(267, 1)
            msg.append_pair(269, 2)
            client_md.logger.info("*" * 50)
            client_md.logger.info("MARKET_DATA_REQUEST (V): SUBSCRIBING")
            client_md.logger.info("*" * 50)
            client_md.send_message(msg)
            client_md.logger.info(f"Subscribed to Trades stream for {symbol}")
            # No snapshot expected for trades stream
            active_md_reqs.append((md_req_id, "trades"))
    # Set a 20-second run for demonstration
    end_time = datetime.now() + timedelta(seconds=20)
    # Ensure state exists
    if state is None:
        state = {
            "symbol": symbol,
            "best_bid": None,
            "best_ask": None,
            "order_book": {"bids": [], "asks": []},
            "book_updates": [],
            "recent_trades": [],
        }
    # Main event loop: process incoming market data and make decisions
    client_md.logger.info(f"Running main loop for { (end_time - datetime.now()).seconds } seconds...")
    next_decision_time = datetime.now()
    while datetime.now() < end_time:
        time.sleep(0.01)
        # Process all queued Market Data messages
        for _ in range(client_md.queue_msg_received.qsize()):
            msg = client_md.queue_msg_received.get()
            msg_type = msg.get("35").decode("utf-8") if msg.get("35") else None
            if msg_type == "X":
                # MarketDataIncrementalRefresh
                sub_id = None if not msg.get(262) else msg.get(262).decode("utf-8")
                updates = 0 if not msg.get(268) else int(msg.get(268).decode("utf-8"))
                sym = None if not msg.get(55) else msg.get(55).decode("utf-8")
                # Identify which stream this update belongs to
                stream_type = None
                for (req_id, stype) in active_md_reqs:
                    if req_id == sub_id:
                        stream_type = stype
                        break
                if stream_type == "depth" or stream_type == "book_ticker":
                    # Depth or Book Ticker incremental updates (bid/ask entries)
                    first_book_id = None if not msg.get(25043) else msg.get(25043).decode("utf-8")
                    last_book_id = None if not msg.get(25044) else msg.get(25044).decode("utf-8")
                    header = f"Subscription: {sub_id} -> {updates} updates for Symbol: {sym}"
                    if first_book_id or last_book_id:
                        header += f" (BookId {first_book_id} -> {last_book_id})"
                    client_md.logger.info(header)
                    # --- Markdown: Symbol-specific parsing workaround ---
                    # Depth/Incremental FIX (MsgType 'X'): some DELETE (279=2) entries **omit 271 (MDEntrySize)**.
                    # If you index repeating fields naively, later entries shift → parsing errors (observed on ETHUSDT/BTCUSDT).
                    # Workaround: maintain a per-message 'qty_missing_offset' and **do not** read 271 on deletes.
                    # Mirrors examples/depth_stream.py and prevents misalignment across entries.
                    # ---------------------------------------------------
                    qty_missing_offset = 0
                    for i in range(updates):
                        action = None if not msg.get(279, i+1) else msg.get(279, i+1).decode("utf-8")
                        update_type = None if not msg.get(269, i+1) else msg.get(269, i+1).decode("utf-8")
                        price = None if not msg.get(270, i+1) else msg.get(270, i+1).decode("utf-8")
                        # For delete actions (279=2), skip reading 271 for this entry
                        qty_index = i + 1 - qty_missing_offset
                        qty = None if not msg.get(271, qty_index) else msg.get(271, qty_index).decode("utf-8")
                        if action == "2":
                            qty_missing_offset += 1
                        qty_str = "" if action == "2" else f"| Qty: {qty}"
                        info = f"Action: {ACTION.get(action, action)} | Update: {UPDATE.get(update_type, update_type)} | Price: {price} {qty_str}"
                        client_md.logger.info(info)
                        # Update state (order book and best bid/ask)
                        if update_type == "0" and price:
                            p = float(price)
                            if action == "0":
                                state["order_book"]["bids"].append((p, float(qty) if qty else 0.0))
                                state["order_book"]["bids"].sort(key=lambda x: x[0], reverse=True)
                            elif action == "1":
                                state["order_book"]["bids"] = [(p, float(qty) if qty else 0.0) if abs(p - b[0]) < 1e-9 else b for b in state["order_book"]["bids"]]
                            elif action == "2":
                                state["order_book"]["bids"] = [b for b in state["order_book"]["bids"] if abs(b[0] - p) > 1e-9]
                        elif update_type == "1" and price:
                            p = float(price)
                            if action == "0":
                                state["order_book"]["asks"].append((p, float(qty) if qty else 0.0))
                                state["order_book"]["asks"].sort(key=lambda x: x[0])
                            elif action == "1":
                                state["order_book"]["asks"] = [(p, float(qty) if qty else 0.0) if abs(p - a[0]) < 1e-9 else a for a in state["order_book"]["asks"]]
                            elif action == "2":
                                state["order_book"]["asks"] = [a for a in state["order_book"]["asks"] if abs(a[0] - p) > 1e-9]
                        # Update best bid/ask
                        if state["order_book"]["bids"]:
                            state["best_bid"] = state["order_book"]["bids"][0][0]
                        if state["order_book"]["asks"]:
                            state["best_ask"] = state["order_book"]["asks"][0][0]
                        # Record incremental event in history
                        state["book_updates"].append({
                            "action": ACTION.get(action, action),
                            "type": UPDATE.get(update_type, update_type),
                            "price": price,
                            "qty": qty
                        })
                elif stream_type == "trades":
                    header = f"Subscription: {sub_id} -> {updates} trade updates for Symbol: {sym}"
                    client_md.logger.info(header)
                    for i in range(updates):
                        update_type = None if not msg.get(269, i+1) else msg.get(269, i+1).decode("utf-8")
                        price = None if not msg.get(270, i+1) else msg.get(270, i+1).decode("utf-8")
                        qty = None if not msg.get(271, i+1) else msg.get(271, i+1).decode("utf-8")
                        trade_id = None if not msg.get(1003, i+1) else msg.get(1003, i+1).decode("utf-8")
                        transact_time = None if not msg.get(60, i+1) else msg.get(60, i+1).decode("utf-8")
                        aggressor_side = None if not msg.get(2446, i+1) else msg.get(2446, i+1).decode("utf-8")
                        side_text = AGGRESSOR_SIDE.get(aggressor_side, aggressor_side)
                        info = f"Update type: {UPDATE.get(update_type, update_type)} | trade_id: {trade_id} | Transaction time: {transact_time} | Price: {price} | Qty: {qty} | Aggressor side: {side_text}"
                        client_md.logger.info(info)
                        # Record trade in state
                        state["recent_trades"].append({
                            "trade_id": trade_id,
                            "price": float(price) if price else None,
                            "qty": float(qty) if qty else None,
                            "time": transact_time,
                            "side": side_text
                        })
            elif msg_type == "0":
                # Heartbeat (skip logging to reduce noise)
                continue
            else:
                # Other message types (ignore or handle as needed)
                continue
        # Process any Order Entry session messages (e.g., Execution Reports)
        for _ in range(client_oe.queue_msg_received.qsize()):
            msg = client_oe.queue_msg_received.get()
            msg_type = msg.get("35").decode("utf-8") if msg.get("35") else None
            if msg_type == "8":
                # Execution Report
                client_oe.logger.info("Received Execution Report (8)")
                ord_status = None if not msg.get(39) else msg.get(39).decode("utf-8")
                cl_ord_id = None if not msg.get(11) else msg.get(11).decode("utf-8")
                exec_type = None if not msg.get(150) else msg.get(150).decode("utf-8")
                text = None if not msg.get(58) else msg.get(58).decode("utf-8")
                client_oe.logger.info(f"ExecReport: ClOrdID={cl_ord_id} Status={ORD_STATUS.get(ord_status, ord_status)} ExecType={exec_type} Info={text}")
            # Skip heartbeats etc.
        # Periodically call strategy decision function
        if datetime.now() >= next_decision_time:
            intents = handle_quote_decision(state)
            for intent in intents:
                if intent.get("type") == "place_order":
                    side = intent.get("side", "").upper()
                    price = intent.get("price")
                    qty = intent.get("qty")
                    symbol_intent = intent.get("symbol", symbol)
                    client_oe.logger.info(f"Intent: Place {side} order | Price: {price} | Qty: {qty} | Symbol: {symbol_intent}")
                    if enable_trading:
                        # Construct and send FIX New Order Single (D) message
                        msg_new = client_oe.create_fix_message_with_basic_header("D")
                        msg_new.append_pair(38, qty)   # Order Qty
                        msg_new.append_pair(40, 2)    # OrdType = LIMIT
                        msg_new.append_pair(11, str(time.time_ns()))  # ClOrdID
                        msg_new.append_pair(44, price)  # Price
                        msg_new.append_pair(54, 1 if side == "BUY" else 2)  # Side
                        msg_new.append_pair(55, symbol_intent)  # Symbol
                        msg_new.append_pair(59, 1)  # TimeInForce = GTC
                        client_oe.send_message(msg_new)
                        client_oe.logger.info("NewOrderSingle (D) sent")
            # Schedule next strategy evaluation
            next_decision_time = datetime.now() + timedelta(seconds=5)
    # End of main loop - cleanup
    # Unsubscribe from all market data streams
    for md_req_id, stype in active_md_reqs:
        msg = client_md.create_fix_message_with_basic_header("V")
        msg.append_pair(262, md_req_id)
        msg.append_pair(263, 2)  # Unsubscribe
        msg.append_pair(264, 1)
        msg.append_pair(266, "Y")
        msg.append_pair(146, 1)
        msg.append_pair(55, symbol)
        msg.append_pair(267, 1)
        msg.append_pair(269, 2)
        client_md.logger.info("*" * 50)
        client_md.logger.info("MARKET_DATA_REQUEST (V): UNSUBSCRIBING")
        client_md.logger.info("*" * 50)
        client_md.send_message(msg)
    # Logout and disconnect both sessions
    client_md.logger.info("LOGOUT (5) - Market Data session")
    client_md.logout()
    client_md.retrieve_messages_until(message_type="5")
    client_md.logger.info("Closing connection (MD session)")
    client_md.disconnect()
    client_oe.logger.info("LOGOUT (5) - Order Entry session")
    client_oe.logout()
    client_oe.retrieve_messages_until(message_type="5")
    client_oe.logger.info("Closing connection (OE session)")
    client_oe.disconnect()

# Sample runtime config (ratu_mm_settings.ini):
# [runtime]
# SYMBOLS = ETHFDUSD
# STREAMS = book_ticker,depth,trades
# MARKET_DEPTH = 50
# RECV_WINDOW_MS = 5000
# ENABLE_TRADING = false
# MD_ENDPOINT = tcp+tls://fix-md.binance.com:9000
# OE_ENDPOINT = tcp+tls://fix-oe.binance.com:9000
