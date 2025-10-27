import sys
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import json, os, threading, traceback
from datetime import datetime, timedelta

LOCK = threading.Lock()
USERS_FILE = os.getenv('USERS_FILE', './users.json')
TRADES_FILE = os.getenv('TRADES_FILE', './trades.json')

def _ensure_files():
    """Создает файлы, если их нет"""
    for path, default in [(USERS_FILE, {}), (TRADES_FILE, [])]:
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default, f, indent=4, ensure_ascii=False)

def _read(path, default):
    """Безопасное чтение JSON"""
    try:
        if not os.path.exists(path):
            _ensure_files()
            return default
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[DB_JSON] Ошибка чтения {path}: {e}")
        traceback.print_exc()
        return default

def _write(path, data):
    """Безопасная запись JSON"""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"[DB_JSON] Ошибка записи {path}: {e}")
        traceback.print_exc()

def load_users(path=None):
    _ensure_files()
    return _read(path or USERS_FILE, {})

def save_users(data, path=None):
    with LOCK:
        _write(path or USERS_FILE, data)

def get_user(uid, path=None):
    return load_users(path).get(str(uid))

def create_default_user(uid, username=None, path=None):
    uid = str(uid)
    users = load_users(path)
    if uid not in users:
        users[uid] = {
            'username': username,
            'api_key': '',
            'api_secret': '',
            'sub_until': None,
            'settings': {
                'RSI_PERIOD': 14, 'RSI_OVERSOLD': 40, 'RSI_OVERBOUGHT': 60,
                'FAST_MA': 50, 'SLOW_MA': 200,
                'MACD_FAST': 8, 'MACD_SLOW': 21, 'MACD_SIGNAL': 5,
                'ORDER_PERCENT': 10.0, 'ORDER_SIZE_USD': 0.0,
                'TP_PCT': 1.0, 'SL_PCT': 0.5
            }
        }
        save_users(users, path)
    return users[uid]

def set_api_keys(uid, api_key, api_secret, path=None):
    uid = str(uid)
    users = load_users(path)
    if uid not in users:
        create_default_user(uid, path=path)
    users[uid]['api_key'] = api_key
    users[uid]['api_secret'] = api_secret
    save_users(users, path)

def set_subscription(uid, days=30, path=None):
    uid = str(uid)
    users = load_users(path)
    if uid not in users:
        create_default_user(uid, path=path)
    users[uid]['sub_until'] = (datetime.utcnow() + timedelta(days=days)).isoformat()
    save_users(users, path)

def is_subscribed(uid, path=None):
    u = get_user(uid, path)
    if not u or not u.get('sub_until'):
        return False
    try:
        return datetime.fromisoformat(u['sub_until']) > datetime.utcnow()
    except Exception:
        return False

def update_setting(uid, key, value, path=None):
    uid = str(uid)
    users = load_users(path)
    if uid not in users:
        create_default_user(uid, path=path)
    users[uid]['settings'][key] = value
    save_users(users, path)

def append_trade(trade, path=None):
    path = path or TRADES_FILE
    with LOCK:
        trades = _read(path, [])
        trades.append(trade)
        _write(path, trades)

def get_trades_for_user(uid, limit=100, path=None):
    trades = _read(path or TRADES_FILE, [])
    uid = str(uid)
    user_trades = [t for t in trades if str(t.get('user_id')) == uid]
    return user_trades[-limit:]

# Создаём файлы при импорте
_ensure_files()
print("[DB_JSON] База данных готова")
