# main_trading_bot.py
# -*- coding: utf-8 -*-
"""
Объединённый торговый модуль + минимальные Telegram-уведомления.
Использует:
 - client.py (BybitClient) — должен быть в той же папке
 - db_json.py — стандартный JSON-DB helper (create_default_user, load_users, save_users,
                get_user, set_api_keys, update_setting, append_trade, get_trades_for_user и т.д.)
 - .env для конфигов: TELEGRAM_TOKEN, ADMIN_ID, DRY_RUN, SYMBOLS, TIMEFRAME, CANDLE_LIMIT, ...
"""

import os
import sys
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import time
import math
import json
import base64
import threading
import logging
import re
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pandas as pd
import ta
import requests
from dotenv import load_dotenv

load_dotenv()

# env / defaults
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
SYMBOLS_ENV = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TIMEFRAME = os.getenv("TIMEFRAME", "5")
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "300"))
QTY_PRECISION_DEFAULT = int(os.getenv("QTY_PRECISION", "6"))
MIN_NOTIONAL = float(os.getenv("MIN_NOTIONAL", "5"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "60"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or 0)

USERS_FILE = os.getenv("USERS_FILE", "./users.json")
TRADES_FILE = os.getenv("TRADES_FILE", "./trades.json")

# default trading parameters (can be overridden per-user via settings)
DEFAULTS = {
    "RSI_PERIOD": int(os.getenv("RSI_PERIOD", "14")),
    "RSI_OVERSOLD": float(os.getenv("RSI_OVERSOLD", "35")),
    "RSI_OVERBOUGHT": float(os.getenv("RSI_OVERBOUGHT", "65")),
    "RSI_CONFIRM": int(os.getenv("RSI_CONFIRM", "1")),
    "FAST_MA": int(os.getenv("FAST_MA", "9")),
    "SLOW_MA": int(os.getenv("SLOW_MA", "21")),
    "ORDER_PERCENT": float(os.getenv("ORDER_PERCENT", "5")),
    "ORDER_SIZE_USD": float(os.getenv("ORDER_SIZE_USD", "0")),
    "MIN_NOTIONAL": MIN_NOTIONAL,
    "QTY_PRECISION": QTY_PRECISION_DEFAULT,
    "TP_PCT": float(os.getenv("TP_PCT", "1.0")),
    "SL_PCT": float(os.getenv("SL_PCT", "0.5")),
    "MACD_FAST": int(os.getenv("MACD_FAST", "12")),
    "MACD_SLOW": int(os.getenv("MACD_SLOW", "26")),
    "MACD_SIGNAL": int(os.getenv("MACD_SIGNAL", "9")),
    "MACD_THRESHOLD": float(os.getenv("MACD_THRESHOLD", "0.0")),
}

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("main_trading_bot")

# import local modules
try:
    import client as client_module
except Exception:
    client_module = None
    logger.warning("client.py not found or failed to import. Trading operations requiring API will fail.")

try:
    import db_json as db
except Exception:
    logger.exception("db_json.py not found or failed to import")
    raise

# Utilities — wrapper around Telegram sendMessage (simple)
def send_message_to_user(chat_id: int, text: str):
    if not TELEGRAM_TOKEN:
        logger.debug("No TELEGRAM_TOKEN configured — would send to %s: %s", chat_id, text)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": int(chat_id), "text": text}
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logger.warning("Telegram send failed %s -> %s: %s", chat_id, r.status_code, r.text[:200])
    except Exception as e:
        logger.exception("Failed to send Telegram message to %s: %s", chat_id, e)

def forward_to_admin(text: str):
    if ADMIN_ID:
        send_message_to_user(ADMIN_ID, text)
    else:
        logger.info("Admin message (no ADMIN_ID): %s", text)

# qty flooring
def floor_qty(qty: float, prec: int = QTY_PRECISION_DEFAULT) -> float:
    try:
        if qty <= 0:
            return 0.0
        factor = 10 ** int(prec)
        return math.floor(float(qty) * factor) / factor
    except Exception:
        return 0.0

# normalize symbol string: accept BTC/USDT, BTC-USDT, BTCUSDT -> BTCUSDT
_sym_re = re.compile(r'[^A-Z0-9]')
def normalize_symbol(sym: str) -> str:
    if not sym:
        return ""
    s = str(sym).upper().strip()
    s = _sym_re.sub("", s)
    return s

# best-effort symbol validation
def validate_symbols_public(symbols: List[str], testnet: bool = False, client_instance=None) -> Tuple[List[str], List[str]]:
    """
    Return (valid, invalid).
    Tries client.get_symbol_info or public instruments-info endpoint.
    """
    valid = []
    invalid = []
    base_public = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"

    for s in symbols:
        ns = normalize_symbol(s)
        if not ns:
            continue
        ok = False
        # 1) try client get_symbol_info if client_instance provided
        try:
            if client_instance is not None and hasattr(client_instance, "get_symbol_info"):
                info = client_instance.get_symbol_info(ns)
                if info and isinstance(info, dict) and info:
                    ok = True
        except Exception:
            pass
        # 2) public instruments-info
        if not ok:
            try:
                params = {"symbol": ns}
                url = base_public + "/v5/market/instruments-info"
                r = requests.get(url, params=params, timeout=6)
                j = r.json() if r.status_code == 200 else {}
                if isinstance(j, dict):
                    res = j.get("result") or j
                    if isinstance(res, dict):
                        items = res.get("list") or []
                    elif isinstance(res, list):
                        items = res
                    else:
                        items = []
                    if items:
                        for it in items:
                            if isinstance(it, dict) and (it.get("symbol") == ns or it.get("name") == ns):
                                ok = True
                                break
            except Exception:
                pass
        if ok:
            valid.append(ns)
        else:
            invalid.append(ns)
    # dedupe preserving order
    def uniq(seq):
        out = []
        seen = set()
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out
    return uniq(valid), uniq(invalid)

# indicators helpers
def calc_macd_hist_series(close: pd.Series, macd_fast: int, macd_slow: int, macd_signal: int):
    fast = close.ewm(span=macd_fast, adjust=False).mean()
    slow = close.ewm(span=macd_slow, adjust=False).mean()
    macd = fast - slow
    signal = macd.ewm(span=macd_signal, adjust=False).mean()
    hist = macd - signal
    return macd, signal, hist

def rsi_series_custom(close: pd.Series, rsi_period: int):
    try:
        return ta.momentum.RSIIndicator(close, window=rsi_period).rsi()
    except Exception:
        delta = close.diff()
        up = delta.clip(lower=0).rolling(rsi_period).mean()
        down = -delta.clip(upper=0).rolling(rsi_period).mean()
        rs = up / down
        return 100 - (100 / (1 + rs))

# main run iteration
def run_trading_iteration():
    users = db.load_users() if hasattr(db, "load_users") else {}
    if not isinstance(users, dict):
        logger.error("load_users did not return a dict")
        return

    for uid_str, u in list(users.items()):
        try:
            uid = int(uid_str)
        except Exception:
            continue

        # subscription check
        sub_until = u.get("sub_until")
        if not sub_until:
            continue
        try:
            if datetime.fromisoformat(str(sub_until)) < datetime.utcnow():
                continue
        except Exception:
            continue

        # keys existence
        api_key_enc = u.get("api_key", "") or ""
        api_secret_enc = u.get("api_secret", "") or ""
        if not api_key_enc or not api_secret_enc:
            # notify user once? skip silently
            continue
        # decode keys (db_json stores base64 per provided code)
        try:
            api_key = db.decode_key(api_key_enc) if hasattr(db, "decode_key") else base64.b64decode(api_key_enc.encode()).decode()
            api_secret = db.decode_key(api_secret_enc) if hasattr(db, "decode_key") else base64.b64decode(api_secret_enc.encode()).decode()
        except Exception:
            api_key = ""
            api_secret = ""

        if not api_key or not api_secret:
            continue

        # prepare client per-user
        testnet_flag = bool((u.get("settings") or {}).get("TESTNET", os.getenv("TESTNET", "false").lower() == "true"))
        client = None
        if client_module is not None:
            try:
                client = client_module.BybitClient(api_key=api_key, api_secret=api_secret, testnet=testnet_flag)
            except Exception as e:
                logger.warning("User %s: failed to create BybitClient: %s", uid, e)
                client = None

        # get per-user symbols or fallback to env
        settings = (u.get("settings") or {}) or {}
        user_symbols = settings.get("symbols") or SYMBOLS_ENV
        if isinstance(user_symbols, str):
            user_symbols = [s.strip() for s in user_symbols.split(",") if s.strip()]
        # normalize and validate best-effort (we only validate once here)
        normalized = [normalize_symbol(s) for s in user_symbols if normalize_symbol(s)]
        if not normalized:
            normalized = SYMBOLS_ENV[:]

        # validate symbols with public API/client; we still iterate over all, but validation reduces useless calls
        valid_syms, invalid_syms = validate_symbols_public(normalized, testnet=testnet_flag, client_instance=client)
        syms_to_use = valid_syms if valid_syms else normalized  # if none validated, try to use original normalized list

        # indicators config
        merged_settings = {**DEFAULTS, **(settings or {})}
        for symbol in syms_to_use:
            try:
                # fetch candles
                df = None
                if client is not None:
                    try:
                        df = client.fetch_ohlcv_df(symbol, interval=TIMEFRAME, limit=CANDLE_LIMIT)
                    except Exception:
                        df = None
                if df is None:
                    # try client_module top-level helper if exists
                    try:
                        if client_module is not None and hasattr(client_module, "fetch_ohlcv_df"):
                            df = client_module.fetch_ohlcv_df(symbol, interval=TIMEFRAME, limit=CANDLE_LIMIT)
                    except Exception:
                        df = None
                if df is None or (hasattr(df, "empty") and df.empty) or ("close" not in df.columns if hasattr(df, "columns") else False):
                    # no data
                    continue

                close = pd.to_numeric(df['close'], errors='coerce')

                # compute indicators
                rsi_s = rsi_series_custom(close, int(merged_settings.get("RSI_PERIOD", DEFAULTS["RSI_PERIOD"])))
                rsi_now = float(rsi_s.iloc[-1]) if len(rsi_s) > 0 and not pd.isna(rsi_s.iloc[-1]) else 50.0

                try:
                    ema_fast = ta.trend.EMAIndicator(close, window=int(merged_settings.get("FAST_MA"))).ema_indicator().iloc[-1]
                    ema_slow = ta.trend.EMAIndicator(close, window=int(merged_settings.get("SLOW_MA"))).ema_indicator().iloc[-1]
                except Exception:
                    ema_fast = close.ewm(span=int(merged_settings.get("FAST_MA")), adjust=False).mean().iloc[-1]
                    ema_slow = close.ewm(span=int(merged_settings.get("SLOW_MA")), adjust=False).mean().iloc[-1]
                trend_up = ema_fast > ema_slow

                macd_fast = int(merged_settings.get("MACD_FAST", DEFAULTS["MACD_FAST"]))
                macd_slow = int(merged_settings.get("MACD_SLOW", DEFAULTS["MACD_SLOW"]))
                macd_signal = int(merged_settings.get("MACD_SIGNAL", DEFAULTS["MACD_SIGNAL"]))
                _, _, macd_hist_series = calc_macd_hist_series(close, macd_fast, macd_slow, macd_signal)
                macd_hist = float(macd_hist_series.iloc[-1]) if len(macd_hist_series) > 0 else 0.0

                # RSI confirm
                RSI_CONFIRM = int(merged_settings.get("RSI_CONFIRM", DEFAULTS["RSI_CONFIRM"]))
                rsi_confirm_buy = False
                rsi_confirm_sell = False
                if RSI_CONFIRM <= len(rsi_s):
                    last_rsi = rsi_s.iloc[-RSI_CONFIRM:]
                    if last_rsi.notnull().all():
                        rsi_confirm_buy = (last_rsi < float(merged_settings.get("RSI_OVERSOLD", DEFAULTS["RSI_OVERSOLD"]))).all()
                        rsi_confirm_sell = (last_rsi > float(merged_settings.get("RSI_OVERBOUGHT", DEFAULTS["RSI_OVERBOUGHT"]))).all()

                price = float(close.iloc[-1])
                # estimate balance
                try:
                    usdt_balance = client.get_balance_usdt() if client is not None and hasattr(client, "get_balance_usdt") else 0.0
                    if usdt_balance is None:
                        usdt_balance = 0.0
                except Exception:
                    usdt_balance = 0.0

                # order size
                order_size_usd_cfg = float(merged_settings.get("ORDER_SIZE_USD", 0))
                if order_size_usd_cfg > 0:
                    order_usd = min(order_size_usd_cfg, usdt_balance) if usdt_balance > 0 else order_size_usd_cfg
                else:
                    order_usd = (usdt_balance * (float(merged_settings.get("ORDER_PERCENT", DEFAULTS["ORDER_PERCENT"])) / 100.0)) if usdt_balance > 0 else 0.0

                raw_qty = order_usd / price if price > 0 else 0.0
                prec = int(merged_settings.get("QTY_PRECISION", DEFAULTS["QTY_PRECISION"]))
                qty = floor_qty(raw_qty, prec=prec)

                buy_ok = rsi_confirm_buy and trend_up and (macd_hist > float(merged_settings.get("MACD_THRESHOLD", DEFAULTS["MACD_THRESHOLD"])))
                sell_ok = rsi_confirm_sell or (not trend_up) or (macd_hist < -float(merged_settings.get("MACD_THRESHOLD", DEFAULTS["MACD_THRESHOLD"])))

                # positions memory stored in users (in-memory) to know if user already has position for symbol
                # use _positions placed in users JSON while runtime (persisted at end)
                if "_positions" not in u:
                    u["_positions"] = {}
                positions = u["_positions"] or {}

                pos = positions.get(symbol)

                # BUY
                if pos is None and buy_ok and qty > 0 and qty * price >= float(merged_settings.get("MIN_NOTIONAL", DEFAULTS["MIN_NOTIONAL"])):
                    trade = {
                        "user_id": uid,
                        "symbol": symbol,
                        "side": "BUY",
                        "price": price,
                        "qty": qty,
                        "pnl": 0.0,
                        "ts": datetime.utcnow().isoformat()
                    }
                    if not DRY_RUN and client is not None and hasattr(client, "place_order"):
                        try:
                            resp = client.place_order("Buy", qty, symbol)
                            trade["order_resp"] = resp
                        except Exception as e:
                            trade["order_resp"] = {"error": str(e)}
                            logger.exception("Order placement error for %s user %s", symbol, uid)
                    db.append_trade(trade)
                    logger.info("TRADE BUY uid=%s symbol=%s qty=%s price=%s", uid, symbol, qty, price)
                    positions[symbol] = {"side": "LONG", "entry_price": price, "qty": qty, "ts": time.time()}
                    # persist notification
                    send_message_to_user(uid, f"🟢 BUY {symbol} price={price:.8f} qty={qty}")
                # SELL
                elif pos is not None and sell_ok:
                    entry_price = pos.get("entry_price", price)
                    qty_to_sell = pos.get("qty", qty)
                    if qty_to_sell * price >= float(merged_settings.get("MIN_NOTIONAL", DEFAULTS["MIN_NOTIONAL"])) and qty_to_sell > 0:
                        trade = {
                            "user_id": uid,
                            "symbol": symbol,
                            "side": "SELL",
                            "price": price,
                            "qty": qty_to_sell,
                            "pnl": 0.0,
                            "ts": datetime.utcnow().isoformat()
                        }
                        if not DRY_RUN and client is not None and hasattr(client, "place_order"):
                            try:
                                resp = client.place_order("Sell", qty_to_sell, symbol)
                                trade["order_resp"] = resp
                            except Exception as e:
                                trade["order_resp"] = {"error": str(e)}
                                logger.exception("Order placement error for SELL %s user %s", symbol, uid)
                        db.append_trade(trade)
                        logger.info("TRADE SELL uid=%s symbol=%s qty=%s price=%s", uid, symbol, qty_to_sell, price)
                        positions[symbol] = None
                        send_message_to_user(uid, f"🔴 SELL {symbol} price={price:.8f} qty={qty_to_sell}")
                    else:
                        # notional too small — just clear position locally
                        positions[symbol] = None

                # save back positions to users (in-memory, also persisted to users.json)
                u["_positions"] = positions

            except Exception as e:
                logger.exception("User %s symbol %s iteration error: %s", uid, symbol, e)
                # continue next symbol

        # persist user (so positions remain while runtime)
        try:
            users_all = db.load_users()
            users_all[str(uid)] = u
            db.save_users(users_all)
        except Exception:
            # fallback if save_users not provided
            try:
                if hasattr(db, "save_users") and hasattr(db, "load_users"):
                    users_all = db.load_users()
                    users_all[str(uid)] = u
                    db.save_users(users_all)
            except Exception:
                logger.exception("Failed to persist users after processing %s", uid)

def main_loop():
    logger.info("Starting main trading loop. DRY_RUN=%s", DRY_RUN)
    # ensure files exist
    try:
        if not os.path.exists(USERS_FILE):
            db.save_users({})
        if not os.path.exists(TRADES_FILE):
            db._write_json(TRADES_FILE, []) if hasattr(db, "_write_json") else open(TRADES_FILE, "w").write("[]")
    except Exception:
        pass

    while True:
        try:
            run_trading_iteration()
        except Exception as e:
            logger.exception("run_trading_iteration crashed: %s", e)
        time.sleep(max(1, SLEEP_SECONDS))

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Stopping main_trading_bot (KeyboardInterrupt).")
