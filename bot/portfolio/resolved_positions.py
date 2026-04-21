import csv


def append_resolved_trade(
    resolved_trades_log,
    open_fields,
    trade_row,
    resolved_yes,
    resolved_no,
    won,
    pnl,
    exit_type,
):
    with open(resolved_trades_log, "a", newline="") as f:
        csv.writer(f).writerow(
            [trade_row.get(k, "") for k in open_fields] + [resolved_yes, resolved_no, won, pnl, exit_type]
        )
