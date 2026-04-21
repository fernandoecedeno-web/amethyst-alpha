"""
Kalshi bot live dashboard — Amethyst Alpha v1.0.0

Two-column layout:
  LEFT   : Portfolio summary + Active Positions
  RIGHT  : Top Signal + Market Structure
  BOTTOM : Status strip

Public API (unchanged from previous version):
  BotDashboard
  snapshot_from_bot_state(...)
  positions_from_open_trades(...)
  candidates_from_ranked(...)

Run standalone to demo:
    python kalshi_rich_dashboard.py
"""
from __future__ import annotations

import re
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any

try:
    from rich import box
    from rich.align import Align
    from rich.console import Console
    from rich.console import Group   # rich < 12: rich.console; rich >= 12: also rich.console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
    _RICH_OK = True
except ImportError:
    # Provide no-op stubs so module-level code doesn't NameError on import
    _RICH_OK = False
    box = None  # type: ignore[assignment]

    class _Stub:  # type: ignore[no-redef]
        def __getattr__(self, _): return self
        def __call__(self, *a, **kw): return self

    Align = Console = Group = Layout = Live = Panel = Rule = Table = Text = _Stub()  # type: ignore


# ── palette ───────────────────────────────────────────────────────────────────
# Hierarchy: header (bright_magenta) > panel titles (bright_magenta) >
#            borders (purple) > labels (dim white) > values (white)

_C_TITLE   = "bold bright_magenta"   # ASCII logo, top signal ticker
_C_HEAD    = "bright_magenta"        # panel titles
_C_HIGHLIGHT = "bold bright_magenta" # edge value, key numbers
_C_SUB     = "magenta"               # subtitle line, secondary text
_C_LABEL   = "dim white"             # row labels throughout
_C_VALUE   = "white"                 # plain values
_C_DIM2    = "grey50"                # timestamps, very dim info
_C_GOOD    = "green"                 # positive / win
_C_BAD     = "red"                   # negative / loss
_C_WARN    = "yellow"                # caution
_C_BORDER  = "purple"                # all panel borders
_C_BORDER2 = "purple"                # (unified — same as _C_BORDER)


# ── ASCII art header ──────────────────────────────────────────────────────────

_ASCII_LOGO = (
    " █████╗ ███╗   ███╗███████╗████████╗██╗  ██╗██╗   ██╗███████╗████████╗\n"
    "██╔══██╗████╗ ████║██╔════╝╚══██╔══╝██║  ██║╚██╗ ██╔╝██╔════╝╚══██╔══╝\n"
    "███████║██╔████╔██║█████╗     ██║   ███████║ ╚████╔╝ ███████╗   ██║   \n"
    "██╔══██║██║╚██╔╝██║██╔══╝     ██║   ██╔══██║  ╚██╔╝  ╚════██║   ██║   \n"
    "██║  ██║██║ ╚═╝ ██║███████╗   ██║   ██║  ██║   ██║   ███████║   ██║   \n"
    "╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚══════╝   ╚═╝   "
)

_SUBTITLE = "ALPHA  v1.0.0"


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _fmt_pnl(v: float, compact: bool = False) -> str:
    if v > 0:
        s = f"+${v:.2f}" if not compact else f"+{v:.2f}"
        return f"[{_C_GOOD}]{s}[/{_C_GOOD}]"
    if v < 0:
        s = f"-${abs(v):.2f}" if not compact else f"{v:.2f}"
        return f"[{_C_BAD}]{s}[/{_C_BAD}]"
    return f"[{_C_DIM2}]${v:.2f}[/{_C_DIM2}]"


def _fmt_dollar(v: float) -> str:
    style = _C_GOOD if v > 0 else (_C_BAD if v < 0 else _C_VALUE)
    return f"[{style}]${v:.2f}[/{style}]"


def _dedup_events(events: list[tuple], limit: int = 6) -> list[tuple]:
    """Collapse consecutive identical (kind, detail) entries into 'x N' lines."""
    out: list[tuple] = []
    i = len(events) - 1
    seen: list[tuple] = []
    while i >= 0 and len(seen) < limit * 3:
        seen.insert(0, events[i])
        i -= 1

    collapsed: list[tuple] = []
    for entry in seen:
        ts, kind, ticker, detail = entry
        if collapsed and collapsed[-1][1] == kind and collapsed[-1][3] == detail:
            prev_ts, prev_kind, prev_ticker, prev_detail, count = (*collapsed[-1], 1) if len(collapsed[-1]) == 4 else collapsed[-1]
            collapsed[-1] = (prev_ts, prev_kind, prev_ticker, prev_detail, count + 1)
        else:
            collapsed.append((ts, kind, ticker, detail, 1))

    # take last `limit` collapsed entries
    for entry in collapsed[-limit:]:
        ts, kind, ticker, detail, count = entry
        if count > 1:
            out.append((ts, kind, ticker, f"x{count}  {detail}"))
        else:
            out.append((ts, kind, ticker, detail))
    return out


def _held_fmt(secs: int) -> str:
    if secs >= 3600:
        h, m = secs // 3600, (secs % 3600) // 60
        return f"{h}h{m}m" if m else f"{h}h"
    if secs >= 60:
        m, s = secs // 60, secs % 60
        return f"{m}m{s}s" if s else f"{m}m"
    return f"{secs}s"


def _uptime_fmt(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60:02d}s"
    h = secs // 3600
    m = (secs % 3600) // 60
    return f"{h}h{m:02d}m"


def _short_ticker(ticker: str, max_len: int = 22) -> str:
    if not ticker or len(ticker) <= max_len:
        return ticker or ""
    parts = ticker.split("-")
    if len(parts) >= 2:
        compact = f"{parts[0]}-{parts[-1]}"
        if len(compact) <= max_len:
            return compact
    head = max(6, max_len - 7)
    return f"{ticker[:head]}…{ticker[-4:]}"


def _parse_scan_stats(events: list[tuple]) -> tuple[int, int, int]:
    for _, kind, ticker, detail in reversed(events):
        if kind == "SYSTEM" and ticker == "SCAN":
            try:
                parts: dict[str, int] = {}
                for seg in detail.split("|"):
                    k, _, v = seg.strip().partition("=")
                    parts[k.strip()] = int(v.strip())
                return parts.get("markets", 0), parts.get("actionable", 0), parts.get("buys", 0)
            except Exception:
                pass
    return 0, 0, 0


def _regime_style(regime: str) -> str:
    r = (regime or "").upper()
    if "ATTACK" in r:
        return _C_GOOD
    if "NO_TRADE" in r or "DEFENSIVE" in r:
        return _C_BAD
    if "MOMENTUM" in r:
        return _C_WARN
    return _C_LABEL


# ── panel renderers ───────────────────────────────────────────────────────────

def _render_header(state: dict) -> Panel:
    cycle       = _safe_int(state.get("cycle"), 0)
    regime      = str(state.get("regime", "—"))
    mode        = str(state.get("mode", "PAPER")).upper()
    connected   = bool(state.get("connected", True))
    uptime      = _safe_int(state.get("uptime_secs"), 0)
    stale       = bool(state.get("stale", False))
    scan_count  = _safe_int(state.get("scan_count"), 0)

    conn_tag = f"[{_C_GOOD}]CONNECTED[/{_C_GOOD}]" if connected else f"[{_C_BAD}]DISCONNECTED[/{_C_BAD}]"
    rg_style = _regime_style(regime)

    logo_text = Text.from_markup(f"[{_C_TITLE}]{_ASCII_LOGO}[/{_C_TITLE}]")

    sub = Text(justify="center")
    sub.append(f"{_SUBTITLE}", style=_C_SUB)
    sub.append("  •  ", style=_C_DIM2)
    sub.append(f"{mode} MODE", style=_C_SUB)
    sub.append("  •  ", style=_C_DIM2)
    sub.append_text(Text.from_markup(conn_tag))
    sub.append("  •  ", style=_C_DIM2)
    sub.append(regime, style=rg_style)
    sub.append("  •  ", style=_C_DIM2)
    sub.append(f"CYCLE #{cycle}", style=_C_VALUE)
    sub.append("  •  ", style=_C_DIM2)
    sub.append(f"SCANS {scan_count}  UP {_uptime_fmt(uptime)}", style=_C_DIM2)
    if stale:
        sub.append("  ⚠ STALE", style=f"bold {_C_BAD}")

    content = Group(
        Align.center(logo_text),
        Text(""),          # spacing above subtitle
        Align.center(sub),
        Text(""),          # spacing below subtitle
    )
    return Panel(content, border_style=_C_BORDER, box=box.DOUBLE_EDGE, padding=(0, 2))


def _render_portfolio(state: dict) -> Panel:
    bankroll = _safe_float(state.get("bankroll"), 0.0)
    equity   = _safe_float(state.get("equity"),   0.0)
    cash     = _safe_float(state.get("cash"),      0.0)
    open_exp = _safe_float(state.get("open"),      0.0)
    realized = _safe_float(state.get("realized"),  0.0)
    closed   = _safe_int(state.get("closed_trades"), 0)
    wins     = _safe_int(state.get("wins"),          0)
    losses   = _safe_int(state.get("losses"),        0)

    total_pnl = equity - bankroll
    wr        = wins / closed if closed > 0 else 0.0
    wr_style  = _C_GOOD if wr >= 0.5 else (_C_WARN if wr >= 0.35 else _C_BAD)
    exp_style = _C_WARN if open_exp > 0 else _C_VALUE

    # Two-column layout: label (dim white, fixed width) | value (white, right-aligned)
    t = Table(box=None, show_header=False, padding=(0, 2), expand=True)
    t.add_column("lbl1", style=_C_LABEL,  no_wrap=True, width=10)
    t.add_column("val1", no_wrap=True, justify="right", min_width=9)
    t.add_column("lbl2", style=_C_LABEL,  no_wrap=True, width=10)
    t.add_column("val2", no_wrap=True, justify="right", min_width=9)

    t.add_row(
        "Bankroll",  f"[{_C_VALUE}]${bankroll:.2f}[/{_C_VALUE}]",
        "Equity",    _fmt_dollar(equity),
    )
    t.add_row(
        "Cash",      f"[{_C_VALUE}]${cash:.2f}[/{_C_VALUE}]",
        "Exposure",  f"[{exp_style}]${open_exp:.2f}[/{exp_style}]",
    )
    t.add_row(
        "Realized",  _fmt_pnl(realized, compact=True),
        "Total PnL", _fmt_pnl(total_pnl, compact=True),
    )
    t.add_row(
        "Trades",    f"[{_C_VALUE}]{closed}[/{_C_VALUE}]",
        "Win Rate",  (
            f"[{wr_style}]{wr:.0%}[/{wr_style}]"
            f"  [{_C_GOOD}]{wins}W[/{_C_GOOD}]"
            f"[{_C_DIM2}]/[/{_C_DIM2}]"
            f"[{_C_BAD}]{losses}L[/{_C_BAD}]"
        ),
    )

    return Panel(t, title=f"[{_C_HEAD}]PORTFOLIO[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 1))


def _render_positions(state: dict, events: list[tuple] | None = None) -> Panel:
    positions = state.get("positions", []) or []

    t = Table(box=None, show_header=True, header_style=_C_LABEL, padding=(0, 1), expand=True)
    t.add_column("Ticker",  no_wrap=True, ratio=3)
    t.add_column("Side",    width=4, no_wrap=True, justify="center")
    t.add_column("Size",    no_wrap=True, justify="right")
    t.add_column("PnL",     no_wrap=True, justify="right", ratio=1)
    t.add_column("Held",    no_wrap=True, justify="right")

    if not positions:
        last_block_reason = str(state.get("last_final_rejection_reason", "") or "").strip()
        if not last_block_reason:
            for _, kind, _ticker, detail in reversed(events or []):
                if kind == "ENTRY_BLOCK" and detail:
                    m = re.search(r"reason=([A-Za-z0-9_:-]+)", detail)
                    last_block_reason = m.group(1) if m else str(detail).strip()
                    break
        empty = Group(
            Align.center(Text("No open positions", style=f"bold {_C_VALUE}")),
            Align.center(
                Text(
                    f"Last block: {last_block_reason}" if last_block_reason else "No recent candidates",
                    style=f"dim italic {_C_DIM2}",
                )
            ),
        )
        return Panel(empty, title=f"[{_C_HEAD}]ACTIVE POSITIONS[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(1, 1))
    else:
        for p in positions:
            side    = str(p.get("side", "?")).upper()
            pnl_v   = _safe_float(p.get("pnl"), 0.0)
            size_v  = _safe_float(p.get("size"), 0.0)
            held_v  = _safe_int(p.get("held_secs"), 0)
            side_mk = f"[{_C_GOOD}]YES[/{_C_GOOD}]" if side == "YES" else f"[{_C_BAD}]NO[/{_C_BAD}]"
            pnl_style = _C_GOOD if pnl_v > 0 else (_C_BAD if pnl_v < 0 else _C_DIM2)
            t.add_row(
                Text(_short_ticker(p.get("ticker", ""), 20), style="bold white"),
                side_mk,
                Text(f"${size_v:.2f}", style=_C_VALUE),
                _fmt_pnl(pnl_v, compact=True),
                Text(_held_fmt(held_v), style=_C_DIM2),
            )

    return Panel(t, title=f"[{_C_HEAD}]ACTIVE POSITIONS[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 1))


def _render_top_signal(state: dict) -> Panel:
    cands = state.get("candidates", []) or []

    if not cands:
        body = Text("\n  no signals\n", style=f"dim italic {_C_DIM2}")
        return Panel(body, title=f"[{_C_HEAD}]TOP SIGNAL[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 1))

    c = cands[0]
    ticker = c.get("ticker") or "—"
    edge   = _safe_float(c.get("edge"),           0.0)
    press  = _safe_float(c.get("pressure_score"), 0.0)
    spread = _safe_float(c.get("spread"),         0.0)
    escore = _safe_float(c.get("entry_score"),    0.0)
    tier   = str(c.get("tier_name", "") or "")
    touch  = _safe_float(c.get("selected_touch"), 0.0)

    # Status classification
    if edge >= 0.40 and press >= 0.55 and spread <= 0.04:
        status_text, status_style = "ACTIONABLE", "bold bright_magenta"
    elif edge >= 0.25 and press >= 0.45:
        status_text, status_style = "WATCHING",   "yellow"
    else:
        status_text, status_style = "WEAK",       "red"

    press_style = _C_VALUE if press >= 0.45 else _C_BAD
    spr_style   = _C_DIM2  if spread <= 0.04 else _C_BAD

    t = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    t.add_column("label", no_wrap=True, width=10)
    t.add_column("value", no_wrap=True)

    t.add_row(
        Text("Ticker",  style=_C_LABEL),
        Text(_short_ticker(ticker, 28), style="bold white"),
    )
    t.add_row(Text("", style=""), Text("", style=""))  # blank row between ticker and metrics
    t.add_row(
        Text("Edge",    style=_C_LABEL),
        Text(f"{edge:.3f}", style="bold bright_magenta"),
    )
    t.add_row(
        Text("Status",  style=_C_LABEL),
        Text(status_text, style=status_style),
    )
    t.add_row(
        Text("Pressure",style=_C_LABEL),
        Text(f"{press:.3f}", style=press_style),
    )
    t.add_row(
        Text("Spread",  style=_C_LABEL),
        Text(f"{spread:.3f}", style=spr_style),
    )
    t.add_row(
        Text("Score",   style=_C_LABEL),
        Text(f"{escore:.3f}", style=_C_VALUE),
    )
    if tier:
        t.add_row(Text("Tier",  style=_C_LABEL), Text(tier,          style=_C_DIM2))
    if touch > 0:
        t.add_row(Text("Touch", style=_C_LABEL), Text(f"{touch:.0f}", style=_C_DIM2))

    if len(cands) > 1:
        t.add_row(Text("", style=""), Text("", style=""))
        t.add_row(Text("", style=""), Text(f"+{len(cands)-1} more", style=_C_DIM2))

    return Panel(t, title=f"[{_C_HEAD}]TOP SIGNAL[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 1))


def _render_market_structure(state: dict) -> Panel:
    cands = state.get("candidates", []) or []

    if not cands:
        body = Text("\n  no market data\n", style=f"dim italic {_C_DIM2}")
        return Panel(body, title=f"[{_C_HEAD}]MARKET STRUCTURE[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 1))

    pressures = [_safe_float(c.get("pressure_score")) for c in cands]
    edges     = [_safe_float(c.get("edge"))           for c in cands]

    avg_press = sum(pressures) / len(pressures) if pressures else 0.0
    best_edge = max(edges)                       if edges     else 0.0

    # Mini-table: top 5 candidates
    t = Table(box=None, show_header=True, header_style=_C_LABEL, padding=(0, 1), expand=True)
    t.add_column("Ticker", no_wrap=True, ratio=3)
    t.add_column("Edge",   no_wrap=True, justify="right", width=6)
    t.add_column("Press",  no_wrap=True, justify="right", width=6)
    t.add_column("Spr",    no_wrap=True, justify="right", width=5)

    for c in cands[:5]:
        e = _safe_float(c.get("edge"),           0.0)
        p = _safe_float(c.get("pressure_score"), 0.0)
        s = _safe_float(c.get("spread"),         0.0)

        # Edge: bright_magenta for best, dim for rest
        e_style = "bold bright_magenta" if e == best_edge else _C_SUB
        p_style = _C_WARN if p >= 0.45 else _C_BAD
        s_style = _C_BAD  if s > 0.04  else _C_DIM2

        t.add_row(
            Text(_short_ticker(c.get("ticker", ""), 16), style=_C_DIM2),
            Text(f"{e:.2f}", style=e_style),
            Text(f"{p:.2f}", style=p_style),
            Text(f"{s:.2f}", style=s_style),
        )

    # Summary footer
    ap_style = _C_WARN if avg_press >= 0.45 else _C_BAD
    be_style = _C_GOOD if best_edge >= 0.40 else (_C_WARN if best_edge >= 0.25 else _C_BAD)
    summary  = Text()
    summary.append("  avg press ", style=_C_LABEL)
    summary.append(f"{avg_press:.2f}", style=ap_style)
    summary.append("   best edge ", style=_C_LABEL)
    summary.append(f"{best_edge:.3f}", style=be_style)

    return Panel(Group(t, summary), title=f"[{_C_HEAD}]MARKET STRUCTURE[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 1))


def _render_status(state: dict, events: list[tuple]) -> Panel:
    mkt, act, buys = _parse_scan_stats(events)
    next_secs  = _safe_int(state.get("next_scan_secs"), 0)
    scanning   = bool(state.get("scanning", False))
    cycle      = _safe_int(state.get("cycle"), 0)
    scan_count = _safe_int(state.get("scan_count"), 0)
    stale      = bool(state.get("stale", False))

    # Most recent non-system event for right side
    last_label, last_ticker, last_detail = "", "", ""
    for _, kind, ticker, detail in reversed(events):
        if kind != "SYSTEM":
            last_label  = {"BUY": "BUY", "EXIT": "EXIT", "ENTRY_BLOCK": "BLOCK", "ROTATION": "ROTATE"}.get(kind, kind)
            last_ticker = _short_ticker(ticker, 16)
            last_detail = (detail or "")[:40]
            break

    # Build single-line status text
    line = Text()
    line.append("cycle ", style=_C_LABEL)
    line.append(f"#{cycle}", style=_C_VALUE)
    line.append("  •  scans ", style=_C_DIM2)
    line.append(f"{scan_count}", style=_C_VALUE)
    line.append("  •  mkt ",  style=_C_DIM2)
    line.append(f"{mkt}",     style=_C_VALUE)
    line.append("  •  act ",  style=_C_DIM2)
    line.append(f"{act}",     style=_C_VALUE)
    line.append("  •  buy ",  style=_C_DIM2)
    line.append(f"{buys}",    style=(_C_GOOD if buys > 0 else _C_VALUE))

    if scanning:
        line.append("  •  ", style=_C_DIM2)
        line.append("SCANNING…", style=_C_WARN)
    elif next_secs > 0:
        line.append("  •  next ", style=_C_DIM2)
        line.append(f"{next_secs}s", style=_C_VALUE)

    if stale:
        line.append("  ⚠ STALE", style=f"bold {_C_BAD}")

    if last_label:
        line.append("     ", style="")
        lbl_style = {"BUY": _C_GOOD, "EXIT": _C_BAD, "BLOCK": _C_WARN, "ROTATE": _C_SUB}.get(last_label, _C_DIM2)
        line.append(f"[{last_label}]", style=lbl_style)
        line.append(f" {last_ticker}", style=_C_VALUE)
        if last_detail:
            line.append(f"  {last_detail}", style=_C_DIM2)

    return Panel(Align.left(line), border_style=_C_BORDER, box=box.MINIMAL, padding=(0, 1))


def _render_tape(events: list[tuple]) -> Panel:
    """Recent event tape — deduplicates repeated lines."""
    _style = {
        "BUY":          _C_GOOD,
        "EXIT":         _C_BAD,
        "ENTRY_BLOCK":  _C_WARN,
        "ROTATION":     _C_SUB,
        "SYSTEM":       _C_DIM2,
    }
    _label = {
        "BUY":          "BUY",
        "EXIT":         "EXIT",
        "ENTRY_BLOCK":  "BLOCK",
        "ROTATION":     "ROTATE",
        "SYSTEM":       "SYS",
    }

    t = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    t.add_column("ts",  width=8, no_wrap=True)
    t.add_column("typ", width=7, no_wrap=True)
    t.add_column("msg", ratio=1, no_wrap=True, overflow="ellipsis")

    shown = _dedup_events(events, limit=6)
    for ts, kind, ticker, detail in shown:
        st  = _style.get(kind, _C_DIM2)
        lbl = _label.get(kind, kind[:6])
        msg = (detail or ticker or "")[:52]
        t.add_row(
            Text(ts,  style=_C_DIM2),
            Text(lbl, style=st),
            Text(msg, style=_C_DIM2),
        )

    if not shown:
        t.add_row(Text("", style=""), Text("waiting…", style=f"dim italic {_C_DIM2}"), Text("", style=""))

    return Panel(t, title=f"[{_C_HEAD}]TAPE[/{_C_HEAD}]", border_style=_C_BORDER, box=box.ROUNDED, padding=(0, 0))


# ── layout factory ─────────────────────────────────────────────────────────────

def _make_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=10),
        Layout(name="body",   ratio=1),
        Layout(name="status", size=3),
    )
    layout["body"].split_row(
        Layout(name="left",  ratio=5),
        Layout(name="right", ratio=4),
    )
    layout["left"].split_column(
        Layout(name="portfolio", size=8),
        Layout(name="positions", ratio=1),
        Layout(name="tape",      size=9),
    )
    layout["right"].split_column(
        Layout(name="topsignal", ratio=2),
        Layout(name="structure", ratio=3),
    )
    return layout


# ── BotDashboard class ─────────────────────────────────────────────────────────

class BotDashboard:
    """Thread-safe live terminal dashboard — Amethyst Alpha v1.0.0.

    Usage:
        db = BotDashboard()
        db.start()
        db.update(snapshot)
        db.add_event("BUY", ticker, "edge=0.41")
        db.stop()
    """

    def __init__(self, refresh_per_second: int = 1):
        self._refresh  = refresh_per_second
        self._state:   dict[str, Any]      = {}
        self._events:  deque[tuple]        = deque(maxlen=200)
        self._lock     = threading.Lock()
        self._stop     = threading.Event()
        self._thread:  threading.Thread | None = None
        # liveness tracking
        self._start_time:           float  = 0.0
        self._scan_count:           int    = 0
        self._scan_ts:              str    = ""
        self._scanning:             bool   = False
        self._next_scan_at:         float  = 0.0
        self._last_state_update_at: float  = 0.0

    # ── public API ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if not _RICH_OK:
            raise ImportError("rich is required: pip install rich")
        self._stop.clear()
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._run, daemon=True, name="dashboard")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def update(self, snapshot: dict) -> None:
        with self._lock:
            self._state = snapshot
            self._last_state_update_at = time.time()

    def add_event(self, kind: str, ticker: str, detail: str = "") -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        with self._lock:
            self._events.append((ts, kind, ticker, detail))
            self._last_state_update_at = time.time()

    def set_scanning(self) -> None:
        with self._lock:
            self._scanning = True
            self._last_state_update_at = time.time()

    def set_idle(self, next_in_secs: int = 10) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        with self._lock:
            self._scanning  = False
            self._scan_ts   = ts
            _now            = time.time()
            self._next_scan_at          = _now + next_in_secs
            self._last_state_update_at  = _now
            self._scan_count           += 1

    # ── internal ───────────────────────────────────────────────────────────────

    def _run(self) -> None:
        layout = _make_layout()
        try:
            with Live(layout, refresh_per_second=self._refresh, screen=True):
                while not self._stop.is_set():
                    now_ts = time.time()
                    with self._lock:
                        state      = dict(self._state)
                        events     = list(self._events)
                        scan_ts    = self._scan_ts
                        nsa        = self._next_scan_at
                        scanning   = self._scanning
                        start_time = self._start_time
                        scan_count = self._scan_count
                        lsu        = self._last_state_update_at

                    # inject live fields
                    state["scan_ts"]        = scan_ts
                    state["next_scan_secs"] = max(0, int(nsa - now_ts)) if nsa > 0 else 0
                    state["scanning"]       = scanning
                    state["scan_stats"]     = _parse_scan_stats(events)
                    state["uptime_secs"]    = int(now_ts - start_time) if start_time > 0 else 0
                    state["scan_count"]     = scan_count
                    state["stale"]          = (now_ts - lsu) > 20 if lsu > 0 else False

                    for _panel, _renderer in (
                        ("header",    lambda: _render_header(state)),
                        ("portfolio", lambda: _render_portfolio(state)),
                        ("positions", lambda: _render_positions(state, events)),
                        ("tape",      lambda: _render_tape(events)),
                        ("topsignal", lambda: _render_top_signal(state)),
                        ("structure", lambda: _render_market_structure(state)),
                        ("status",    lambda: _render_status(state, events)),
                    ):
                        try:
                            layout[_panel].update(_renderer())
                        except Exception:
                            pass  # one bad panel must never kill the loop

                    time.sleep(1.0)
        except Exception:
            pass  # dashboard crash must never kill the bot


# ── bot-state adapter helpers ──────────────────────────────────────────────────

def positions_from_open_trades(
    open_rows: list[dict],
    prices: dict,
    safe_float,
    now,
    normalize_timestamp_utc,
) -> list[dict]:
    """Convert open_rows + live prices into dashboard position dicts."""
    out = []
    for row in open_rows:
        ticker = row.get("ticker", "")
        side   = row.get("side", "?").upper()
        size   = safe_float(row.get("position_usd"), 0.0) or 0.0
        entry  = safe_float(row.get("crowd_prob"), None)
        p      = prices.get(ticker, {})
        if side == "NO":
            cur = safe_float(p.get("no_bid"),  safe_float(p.get("no_ask"),  entry))
        else:
            cur = safe_float(p.get("yes_bid"), safe_float(p.get("yes_ask"), entry))
        pnl = round(size * ((cur - entry) if (cur is not None and entry is not None) else 0.0), 2)
        try:
            edt  = normalize_timestamp_utc(row.get("timestamp", ""))
            held = int((now - edt).total_seconds()) if edt else 0
        except Exception:
            held = 0
        parts = ticker.split("-")
        short = f"{parts[0]}-{parts[-1]}" if len(parts) >= 3 else ticker
        out.append({
            "ticker":     short,
            "side":       side,
            "size":       size,
            "pnl":        pnl,
            "held_secs":  held,
            "conviction": row.get("conviction_state", ""),
        })
    return out


def candidates_from_ranked(ranked_candidates: list[dict]) -> list[dict]:
    """Slim down ranked_candidates to what the dashboard needs."""
    out = []
    for c in ranked_candidates[:8]:
        out.append({
            "ticker":         c.get("ticker", ""),
            "edge":           c.get("edge", 0.0),
            "quality_score":  c.get("quality_score", 0.0),
            "pressure_score": c.get("pressure_score", 0.0),
            "spread":         c.get("spread", 0.0),
            "entry_score":    c.get("entry_score", 0.0),
            "tier_name":      c.get("tier_name", ""),
            "selected_touch": c.get("selected_touch", 0.0),
        })
    return out


def snapshot_from_bot_state(
    bankroll: float,
    cash_balance: float,
    open_exposure: float,
    realized_pnl: float,
    cycle_num: int,
    regime: str,
    open_rows: list[dict],
    ranked_candidates: list[dict],
    prices: dict,
    safe_float,
    now,
    normalize_timestamp_utc,
    closed_trades: int = 0,
    wins: int = 0,
    losses: int = 0,
    last_final_rejection_reason: str = "",
) -> dict:
    """Build a complete dashboard snapshot from bot state."""
    return {
        "bankroll":      bankroll,
        "equity":        round(cash_balance + open_exposure, 2),
        "cash":          cash_balance,
        "open":          open_exposure,
        "realized":      realized_pnl,
        "cycle":         cycle_num,
        "regime":        regime,
        "positions":     positions_from_open_trades(open_rows, prices, safe_float, now, normalize_timestamp_utc),
        "candidates":    candidates_from_ranked(ranked_candidates),
        "closed_trades": closed_trades,
        "wins":          wins,
        "losses":        losses,
        "last_final_rejection_reason": last_final_rejection_reason,
    }


# ── standalone demo ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import random

    db = BotDashboard()
    db.update({
        "bankroll":      90.00,
        "equity":        84.52,
        "cash":          70.92,
        "open":          13.60,
        "realized":     -5.48,
        "cycle":         42,
        "regime":        "MOMENTUM_NORMAL",
        "mode":          "PAPER",
        "connected":     True,
        "closed_trades": 25,
        "wins":          8,
        "losses":        17,
        "positions": [
            {"ticker": "KXBTC-B75625", "side": "YES", "size": 8.0,  "pnl":  0.24, "held_secs": 142, "conviction": "strong"},
            {"ticker": "KXETH-E1800",  "side": "NO",  "size": 5.60, "pnl": -0.48, "held_secs":  88, "conviction": "neutral"},
        ],
        "candidates": [
            {"ticker": "KXBTC-26APR1917-B75625", "edge": 0.38, "pressure_score": 0.61, "spread": 0.02, "entry_score": 0.47, "tier_name": "T1", "selected_touch": 22},
            {"ticker": "KXETH-26APR1917-E1820",  "edge": 0.29, "pressure_score": 0.44, "spread": 0.03, "entry_score": 0.39, "tier_name": "T2", "selected_touch": 11},
            {"ticker": "KXINX-26APR-5400",        "edge": 0.22, "pressure_score": 0.51, "spread": 0.04, "entry_score": 0.35, "tier_name": "T3", "selected_touch": 8},
        ],
    })
    db.add_event("BUY",         "KXBTC-26APR1917-B75625", "edge=0.41 size=8.0")
    db.add_event("EXIT",        "KXBTC-24900",             "time_stop pnl=-0.01")
    db.add_event("ENTRY_BLOCK", "KXETH-1780",              "low_pressure")
    db.add_event("SYSTEM",      "SCAN",                    "markets=24 | actionable=3 | buys=1")

    db.start()
    db.set_idle(next_in_secs=10)

    try:
        i = 0
        while True:
            time.sleep(3)
            db.add_event("ROTATION", "KXBTC-25001", f"alpha={random.uniform(-0.05, 0.12):.3f}")
            i += 1
            if i % 3 == 0:
                db.add_event("SYSTEM", "SCAN", f"markets={random.randint(10,30)} | actionable={random.randint(1,5)} | buys={random.randint(0,2)}")
                db.set_idle(next_in_secs=10)
    except KeyboardInterrupt:
        db.stop()
