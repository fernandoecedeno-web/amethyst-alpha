from bot.utils import normalize_timestamp_utc


def print_cycle_header(log, title, bankroll, min_ev, min_crowd, max_crowd, take_profit, stop_loss, selection_min_minutes, selection_max_minutes):
    log.info("=" * 50)
    log.info(f"  {title}")
    log.info(
        f"  Bankroll:${bankroll} | MinEV:{min_ev:.1%} | Crowd:{min_crowd:.0%}-{max_crowd:.0%}"
        f" | TP:{take_profit:.1%}/SL:{stop_loss:.1%} | YES-MOMENTUM | PAPER MODE"
    )
    log.info(
        f"SELECTION WINDOW | min_close_minutes={selection_min_minutes}"
        f" max_close_minutes={selection_max_minutes} ({selection_max_minutes//60}h)"
    )
    log.info("=" * 50)


def print_balance_summary(log, cyan, yellow, fmt_money, bankroll, cash_balance, open_exposure, open_count, all_w, all_l, all_wr, all_pnl, session_wins, session_losses, session_wr, session_pnl, realized_cash_pnl=0.0):
    bar = "─" * 72
    equity = cash_balance + open_exposure
    log.info(cyan(bar))
    log.info(
        f" START ${bankroll:.2f}"
        f"  │  EQUITY {cyan(f'${equity:.2f}')}"
        f"  │  CASH ${cash_balance:.2f}"
        f"  │  OPEN {yellow(f'${open_exposure:.2f}')} ({open_count})"
        f"  │  REALIZED {fmt_money(realized_cash_pnl)}"
    )
    log.info(f" ALL  W:{all_w} L:{all_l}  WR:{all_wr:.1%}  PnL:{fmt_money(all_pnl, 0)}")
    log.info(f" NOW  W:{session_wins}  L:{session_losses}  WR:{session_wr:.1%}  PnL:{fmt_money(session_pnl)}")
    log.info(cyan(bar))
    return bar


def print_exposure_summary(log, fmt_money, open_rows, prices, now, safe_float, timezone, datetime):
    if open_rows:
        for row in open_rows:
            ticker = row.get("ticker", "")
            side = row.get("side", "?").upper()
            size = safe_float(row.get("position_usd"), 0.0) or 0.0
            entry = safe_float(row.get("crowd_prob"), None)
            parts = ticker.split("-")
            short = f"{parts[0]}-{parts[-1]}" if len(parts) >= 3 else ticker
            p = prices.get(ticker, {})
            if side == "NO":
                cur = safe_float(p.get("no_bid"), safe_float(p.get("no_ask"), entry))
            else:
                cur = safe_float(p.get("yes_bid"), safe_float(p.get("yes_ask"), entry))
            pnl_now = round(size * ((cur - entry) if (cur is not None and entry is not None) else 0.0), 2)
            try:
                edt = normalize_timestamp_utc(row.get("timestamp", ""))
                if edt is None:
                    raise ValueError("missing timestamp")
                held = int((now - edt).total_seconds())
            except Exception:
                held = 0
            log.info(f"  {short:<18} {side:<3}  ${size:.2f}  {fmt_money(pnl_now)}  {held}s")
    else:
        log.info("  (no open positions)")


def print_skip_reason_summary(log, separator, skip_pairs, signals, cycle_num):
    log.info("SKIP REASONS:")
    for label, val in skip_pairs:
        log.info(f"  {label} : {val}")
    log.info(separator)
    log.info(f"Cycle #{cycle_num} complete — {signals} signal(s)")
    log.info(separator)


def print_candidate_summary(log, cyan, adapt_evaluated, adapt_touch_pass, adapt_spread_pass, adapt_edge_pass, adapt_quality_pass, adapt_near_miss, passed_candidates, ranked_candidates, attempts, buys, skip_cluster, blocked_capital, blocked_open_cap, rotations):
    attempted = 0
    try:
        attempted = attempts
    except Exception:
        attempted = 0
    log.info(cyan(
        f"ADAPT_SUMMARY | evaluated={adapt_evaluated} touch_pass={adapt_touch_pass} spread_pass={adapt_spread_pass}"
        f" edge_pass={adapt_edge_pass} quality_pass={adapt_quality_pass} near_miss={adapt_near_miss}"
    ))
    log.info(cyan(
        f"EXEC_SUMMARY | passed={passed_candidates} ranked={ranked_candidates} attempted={attempted}"
        f" bought={buys} blocked_cluster={skip_cluster} blocked_capital={blocked_capital}"
        f" blocked_open_cap={blocked_open_cap} rotated={rotations}"
    ))


def print_exit_summary(log, hold_deferred, exit_counts):
    log.info(
        f"EXIT_SUMMARY | deferred={hold_deferred} tp={exit_counts.get('tp_hit',0)}"
        f" pressure={exit_counts.get('pressure_failure',0)} momentum={exit_counts.get('momentum_break',0)}"
        f" hard_stop={exit_counts.get('hard_stop',0)} trail={exit_counts.get('trail_protect',0)}"
        f" time={exit_counts.get('time_stop',0)} broken={exit_counts.get('exec_deterioration',0)}"
        f" stale_conv={exit_counts.get('stale_conviction',0)} stale_break={exit_counts.get('stale_break',0)}"
        f" conviction_decay={exit_counts.get('conviction_decay',0)}"
    )
