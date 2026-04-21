#!/usr/bin/env python3
"""
replay_engine.py — Replay and filter trade log files.

Supported formats: .log, .jsonl, .json, .csv

Schema auto-detection:
  resolved_trades mode — activated when 'pnl_usd' and 'won' columns are present.
    PnL source : pnl_usd
    Win source : won == 1
    Edge field : ev (used for --min-ev)
    Extra group: exit_type
  generic mode — original behaviour.
    PnL proxy  : net_edge → raw_edge → 0
    Win source : pnl > 0
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path


# ── Field lists ───────────────────────────────────────────────────────────────

# Fields parsed as float in both modes
GENERIC_NUMERIC = ("spread", "pressure", "eqs", "raw_edge", "net_edge", "price")
GENERIC_STRING  = ("side", "family", "strategy")

RESOLVED_NUMERIC = ("ev", "pnl_usd", "crowd_prob", "model_prob",
                    "position_usd", "hours_to_close")
RESOLVED_STRING  = ("side", "family", "exit_type", "ticker", "tier")
RESOLVED_INT     = ("won",)


# ── Schema detection ──────────────────────────────────────────────────────────

def detect_mode(raw_records):
    """Return 'resolved' if pnl_usd + won are present, else 'generic'."""
    for rec in raw_records[:20]:
        if isinstance(rec, dict) and "pnl_usd" in rec and "won" in rec:
            return "resolved"
    return "generic"


# ── Record parsers ────────────────────────────────────────────────────────────

def parse_generic(raw):
    """Parse a raw dict for generic (log/jsonl) mode. Returns None if unusable."""
    if not isinstance(raw, dict):
        return None
    rec = {"_mode": "generic"}
    for field in GENERIC_NUMERIC:
        val = raw.get(field)
        rec[field] = _to_float(val)
    for field in GENERIC_STRING:
        val = raw.get(field)
        rec[field] = str(val).strip().lower() if val is not None else None
    return rec


def parse_resolved(raw):
    """Parse a raw dict for resolved_trades CSV mode. Returns None if unusable."""
    if not isinstance(raw, dict):
        return None
    rec = {"_mode": "resolved"}
    for field in RESOLVED_NUMERIC:
        rec[field] = _to_float(raw.get(field))
    for field in RESOLVED_STRING:
        val = raw.get(field)
        rec[field] = str(val).strip().lower() if val not in (None, "") else None
    for field in RESOLVED_INT:
        rec[field] = _to_int(raw.get(field))
    # Expose ev also as raw_edge / net_edge so generic filters still work
    rec["raw_edge"] = rec["ev"]
    rec["net_edge"] = rec["ev"]
    return rec


def _to_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _to_int(val):
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


# ── File loaders ──────────────────────────────────────────────────────────────

def load_jsonl(path):
    records = []
    with open(path, "r") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  [warn] line {lineno}: skipping bad JSON — {e}", file=sys.stderr)
    return records


def load_json(path):
    with open(path, "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("trades", "rows", "records", "data", "results"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def load_csv(path):
    records = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    return records


def load_log(path):
    """Try JSON-per-line first, then fall back to key=value pairs."""
    records = []
    with open(path, "r") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
                continue
            except json.JSONDecodeError:
                pass
            rec = {}
            for token in line.split():
                if "=" in token:
                    k, _, v = token.partition("=")
                    rec[k.strip()] = v.strip()
            if rec:
                records.append(rec)
            else:
                print(f"  [warn] line {lineno}: could not parse — skipping", file=sys.stderr)
    return records


def load_file(path):
    suffix = path.suffix.lower()
    if suffix == ".log":
        return load_log(path)
    if suffix == ".jsonl":
        return load_jsonl(path)
    if suffix == ".json":
        return load_json(path)
    if suffix == ".csv":
        return load_csv(path)
    print(f"  [warn] unknown extension '{suffix}', trying jsonl then csv", file=sys.stderr)
    try:
        return load_jsonl(path)
    except Exception:
        return load_csv(path)


# ── Filtering ─────────────────────────────────────────────────────────────────

def apply_filters(raw_records, args, mode):
    parser_fn = parse_resolved if mode == "resolved" else parse_generic
    kept = []
    skipped = 0

    for raw in raw_records:
        rec = parser_fn(raw)
        if rec is None:
            skipped += 1
            continue

        try:
            # ── Filters available in both modes ──────────────────────────────
            if args.only_side:
                if rec.get("side") != args.only_side.lower():
                    continue
            if args.only_family:
                if rec.get("family") != args.only_family.lower():
                    continue

            # ── resolved_trades filters ───────────────────────────────────────
            if mode == "resolved":
                if args.min_ev is not None and rec["ev"] is not None:
                    if rec["ev"] < args.min_ev:
                        continue
                if args.only_exit_type:
                    if rec.get("exit_type") != args.only_exit_type.lower():
                        continue

            # ── generic filters ───────────────────────────────────────────────
            if mode == "generic":
                if args.max_spread is not None and rec.get("spread") is not None:
                    if rec["spread"] > args.max_spread:
                        continue
                if args.min_pressure is not None and rec.get("pressure") is not None:
                    if rec["pressure"] < args.min_pressure:
                        continue
                if args.min_eqs is not None and rec.get("eqs") is not None:
                    if rec["eqs"] < args.min_eqs:
                        continue
                if args.min_raw_edge is not None and rec.get("raw_edge") is not None:
                    if rec["raw_edge"] < args.min_raw_edge:
                        continue
                if args.min_net_edge is not None and rec.get("net_edge") is not None:
                    if rec["net_edge"] < args.min_net_edge:
                        continue
                if args.only_strategy:
                    if rec.get("strategy") != args.only_strategy.lower():
                        continue

        except Exception:
            skipped += 1
            continue

        kept.append(rec)

    if skipped:
        print(f"  [warn] skipped {skipped} unparseable rows", file=sys.stderr)
    return kept


# ── Stats computation ─────────────────────────────────────────────────────────

def compute_stats(records):
    """
    Compute stats for a list of parsed records.
    resolved mode: uses pnl_usd and won.
    generic mode:  uses net_edge/raw_edge proxy and pnl > 0.
    """
    total = len(records)
    if total == 0:
        return {
            "total": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "total_pnl": 0.0,
            "avg_pnl": 0.0, "avg_win_pnl": 0.0, "avg_loss_pnl": 0.0,
        }

    wins = 0
    losses = 0
    total_pnl = 0.0
    win_pnl_sum = 0.0
    loss_pnl_sum = 0.0

    for rec in records:
        mode = rec.get("_mode", "generic")

        if mode == "resolved":
            pnl = rec.get("pnl_usd") or 0.0
            won = rec.get("won")
            is_win = (won == 1)
        else:
            pnl = rec.get("net_edge") or rec.get("raw_edge") or 0.0
            is_win = pnl > 0

        total_pnl += pnl
        if is_win:
            wins += 1
            win_pnl_sum += pnl
        else:
            losses += 1
            loss_pnl_sum += pnl

    return {
        "total":        total,
        "wins":         wins,
        "losses":       losses,
        "win_rate":     wins / total * 100,
        "total_pnl":    total_pnl,
        "avg_pnl":      total_pnl / total,
        "avg_win_pnl":  win_pnl_sum / wins if wins else 0.0,
        "avg_loss_pnl": loss_pnl_sum / losses if losses else 0.0,
    }


# ── Output ────────────────────────────────────────────────────────────────────

def fmt(val):
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.4f}"


def print_stats(label, stats, indent=0):
    pad = "  " * indent
    print(f"{pad}{label}")
    print(f"{pad}  Trades        : {stats['total']}")
    print(f"{pad}  Wins / Losses : {stats['wins']} / {stats['losses']}")
    print(f"{pad}  Win rate      : {stats['win_rate']:.1f}%")
    print(f"{pad}  Total PnL     : {fmt(stats['total_pnl'])}")
    print(f"{pad}  Avg PnL/trade : {fmt(stats['avg_pnl'])}")
    if stats["wins"]:
        print(f"{pad}  Avg PnL/win   : {fmt(stats['avg_win_pnl'])}")
    if stats["losses"]:
        print(f"{pad}  Avg PnL/loss  : {fmt(stats['avg_loss_pnl'])}")


def grouped_summary(records, field):
    groups = defaultdict(list)
    unlabeled = []
    for rec in records:
        key = rec.get(field)
        if key:
            groups[key].append(rec)
        else:
            unlabeled.append(rec)
    return groups, unlabeled


def print_group(title, records, field):
    print(f"\n{'=' * 50}")
    print(title)
    print("=" * 50)
    groups, unlabeled = grouped_summary(records, field)
    if groups:
        for key in sorted(groups):
            print_stats(key, compute_stats(groups[key]), indent=1)
        if unlabeled:
            print_stats("(unknown)", compute_stats(unlabeled), indent=1)
    else:
        print(f"  (no {field} data)")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Replay and filter a trade log file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Filters active in resolved_trades mode (pnl_usd + won columns present):
  --min-ev, --only-exit-type, --only-side, --only-family

Filters active in generic mode (.log / .jsonl):
  --max-spread, --min-pressure, --min-eqs, --min-raw-edge,
  --min-net-edge, --only-side, --only-family, --only-strategy
""",
    )
    parser.add_argument("log_file", help="Path to file (.log, .jsonl, .json, .csv)")

    # ── resolved_trades filters
    parser.add_argument("--min-ev",         type=float, metavar="N",
                        help="[resolved] Keep rows where ev >= N")
    parser.add_argument("--only-exit-type", type=str,   metavar="S",
                        help="[resolved] Keep only this exit_type (e.g. early_win, early_loss)")

    # ── shared filters
    parser.add_argument("--only-side",      type=str,   metavar="S",
                        help="Keep only this side (yes / no)")
    parser.add_argument("--only-family",    type=str,   metavar="S",
                        help="Keep only this market family (e.g. KXBTC, KXETH)")

    # ── generic filters
    parser.add_argument("--max-spread",     type=float, metavar="N",
                        help="[generic] Keep rows where spread <= N")
    parser.add_argument("--min-pressure",   type=float, metavar="N",
                        help="[generic] Keep rows where pressure >= N")
    parser.add_argument("--min-eqs",        type=float, metavar="N",
                        help="[generic] Keep rows where eqs >= N")
    parser.add_argument("--min-raw-edge",   type=float, metavar="N",
                        help="[generic] Keep rows where raw_edge >= N")
    parser.add_argument("--min-net-edge",   type=float, metavar="N",
                        help="[generic] Keep rows where net_edge >= N")
    parser.add_argument("--only-strategy",  type=str,   metavar="S",
                        help="[generic] Keep only this strategy")

    args = parser.parse_args()

    path = Path(args.log_file)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    print(f"\nLoading: {path}")
    raw_records = load_file(path)
    print(f"  Raw rows loaded : {len(raw_records)}")

    mode = detect_mode(raw_records)
    print(f"  Schema mode     : {mode}")

    records = apply_filters(raw_records, args, mode)
    print(f"  Rows after filters: {len(records)}\n")

    # ── Overall summary ────────────────────────────────────────────────────────
    print("=" * 50)
    print("OVERALL SUMMARY")
    print("=" * 50)
    overall = compute_stats(records)
    print_stats("All filtered trades", overall)

    if not records:
        print("\nNo trades matched the filters.")
        return

    # ── Group breakdowns ───────────────────────────────────────────────────────
    print_group("BY SIDE",      records, "side")
    print_group("BY FAMILY",    records, "family")

    if mode == "resolved":
        print_group("BY EXIT TYPE", records, "exit_type")
    else:
        print_group("BY STRATEGY",  records, "strategy")

    print()


if __name__ == "__main__":
    main()
