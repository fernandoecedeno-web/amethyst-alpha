import logging
import re
from datetime import datetime

from bot.config import COLOR_LOGS, COMPACT_LOGS, DEBUG


RESET = "\033[0m"
BOLD  = "\033[1m"
_ANSI_RE = re.compile(r'\033\[[0-9;]*m')


def color_text(text, code, bold=False):
    text = str(text)
    if not COLOR_LOGS:
        return text
    prefix = f"{BOLD}{code}" if bold else code
    return f"{prefix}{text}{RESET}"


def green(x):   return color_text(x, "\033[92m")
def red(x):     return color_text(x, "\033[91m")
def yellow(x):  return color_text(x, "\033[93m")
def cyan(x):    return color_text(x, "\033[96m")
def magenta(x): return color_text(x, "\033[95m")


def pnl_color(value):
    if value > 0: return "green"
    if value < 0: return "red"
    return None


def _coerce_float(value, default=0.0):
    try:    return float(value)
    except: return default


def fmt_pnl(x, decimals=2):
    value = _coerce_float(x, 0.0)
    if value > 0: return green(f"+{value:.{decimals}f}")
    if value < 0: return red(f"{value:.{decimals}f}")
    return f"{value:.{decimals}f}"


def fmt_money(x, decimals=2):
    value = _coerce_float(x, 0.0)
    text  = f"${value:+,.{decimals}f}"
    color = pnl_color(value)
    return color_text(text, color) if color else text


# ── compressed log helpers ────────────────────────────────────────────────────

_SKIP_CODES: dict[str, str] = {
    "weak_pressure":          "P",
    "low_pressure":           "P",
    "no_pressure":            "P",
    "pressure_exit":          "P",
    "low_edge":               "E",
    "edge_floor":             "E",
    "edge_gate":              "E",
    "bad_liquidity":          "L",
    "low_liquidity":          "L",
    "no_real_book":           "L",
    "no_real_liquidity":      "L",
    "insufficient_depth":     "L",
    "wide_spread":            "S",
    "spread_too_wide":        "S",
    "low_volume":             "V",
    "low_quality":            "Q",
    "bad_regime_low_quality": "Q",
    "missing_orderbook":      "B",
    "too_close_to_expiry":    "T",
    "bad_price_range":        "X",
    "stale_data":             "Z",
    "exceeds_2pct_risk":      "R",
    "max_positions_reached":  "R",
    "cooldown":               "C",
    "cooldown_short":         "C",
    "reentry_blk":            "C",
    "duplicate_ticker":       "D",
    "cluster_hard_cap":       "K",
    "cycle_cap":              "K",
    "exposure_cap":           "K",
    "family_exposure":        "K",
}


def skip_code(reason: str) -> str:
    return _SKIP_CODES.get(reason, "?")


def log_skip_compact(log, ticker: str, reason: str) -> None:
    log.info(yellow(f"SKIP {skip_code(reason)}"))


def log_enter(log, ticker: str, side: str, edge: float) -> None:
    log.info(green(f"ENTER {ticker}-{side.upper()} e={edge:.2f}"))


def log_exit(log, ticker: str, pnl_pct: float) -> None:
    sign = "+" if pnl_pct >= 0 else ""
    msg  = f"EXIT {ticker} pnl={sign}{pnl_pct*100:.1f}%"
    log.info(green(msg) if pnl_pct >= 0 else red(msg))


def log_cycle(log, cycle_num: int, bankroll: float, open_pos: int, risk_pct: float) -> None:
    log.info(cyan(f"CYCLE #{cycle_num}"))
    log.info(cyan(f"BANKROLL: ${bankroll:.2f} | OPEN: {open_pos} | RISK: {risk_pct:.1f}%"))


def log_top(log, candidates, limit: int = 5) -> None:
    if not candidates:
        return
    if DEBUG:
        log.info(f"LOG_TOP RECEIVED {len(candidates)} candidates")
    log.info(cyan("TOP:"))
    for idx, c in enumerate(candidates[:limit], 1):
        try:
            ticker = str(c.get("ticker", "NA"))
            side = str(c.get("side", "NA")).upper()
            edge = float(c.get("edge", 0.0))
        except Exception as e:
            log.info(f"TOP ERROR: {e} | raw={c}")
            continue
        if DEBUG:
            log.info(f"LOG_TOP ROW {idx} ticker={ticker} side={side} edge={edge:.2f}")
        log.info(f"{idx}. {ticker}-{side} e={edge:.2f}")


class _CompactStreamFilter(logging.Filter):
    _ALLOWED_PREFIXES = (
        "CYCLE",
        "BANKROLL",
        "TOP",
        "ENTER",
        "EXIT",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        raw = str(record.msg).lstrip()
        if raw[:1].isdigit() and ". " in raw[:4]:
            return True
        return any(raw.startswith(p) for p in self._ALLOWED_PREFIXES)


# ── setup ─────────────────────────────────────────────────────────────────────

def setup_logger(name):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    if COMPACT_LOGS:
        stream_handler.addFilter(_CompactStreamFilter())

    file_handler = logging.FileHandler(f"hci_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger
