"""Shorthand for pulling operational data off the mainnet host for local analysis.

None of this is deployment tooling -- it's a one-way, read-only pull of logs/
snapshots/the price DB for whatever local digging you're doing (backtests,
live-vs-shadow comparisons, incident review). Uses rsync rather than scp so
repeated runs are incremental: the exchange_state snapshot directory only
grows, and re-pulling the whole thing every time wastes bandwidth for no
reason once most of it is already local.

CLI:
    python -m scripts.copier intent            # logs/intent_mainnet.jsonl + shadow
    python -m scripts.copier exchange_state    # exchange_state snapshots (incremental)
    python -m scripts.copier db                # data/pricing/ohlcv_data.duckdb
    python -m scripts.copier all               # all of the above

Or import directly, e.g. from a notebook or another script:
    from scripts.copier import copy_intent, copy_exchange_state, copy_price_db
    copy_intent()
"""
import subprocess
import sys
from pathlib import Path

REMOTE_HOST = "evpn_vps"
REMOTE_ROOT = "/home/ev/projects/trend_trader"

# cd to repo root so relative paths below resolve the same way regardless of
# where this is invoked from (matches the pattern the backtest/analysis
# scripts already use).
_root = Path(__file__).resolve().parent.parent


def _rsync(relative_path: str, required: bool = True) -> None:
    """rsync a file or directory from the host, mirroring its relative path locally.

    A trailing slash on relative_path syncs a directory's CONTENTS into the
    matching local directory (rather than nesting it one level deeper) --
    pass it explicitly the same way on both sides, same as plain rsync.
    """
    remote = f"{REMOTE_HOST}:{REMOTE_ROOT}/{relative_path}"
    local = _root / relative_path
    local.parent.mkdir(parents=True, exist_ok=True)

    cmd = ["rsync", "-avz", "--progress", remote, str(local)]
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.returncode != 0:
        if required:
            raise RuntimeError(f"rsync failed ({result.returncode}): {remote} -> {local}")
        print(f"(skipping -- {relative_path} not found or copy failed, "
              f"fine if it's not currently expected to exist)")
        return
    print(f"OK: {local}")


def copy_intent() -> None:
    """logs/intent_mainnet.jsonl (primary) + logs/intent_mainnet_shadow.jsonl (shadow,
    only present while SHADOW_SIZING is on -- not required)."""
    _rsync("logs/intent_mainnet.jsonl")
    _rsync("logs/intent_mainnet_shadow.jsonl", required=False)


def copy_exchange_state() -> None:
    """Exchange-state snapshots -- incremental, rsync only pulls new/changed files."""
    _rsync("data/hyperliquid_exchange_state_mainnet/")


def copy_price_db() -> None:
    """The live Hyperliquid OHLCV DuckDB. Large (400MB+) -- rsync's delta transfer
    means a re-pull after the first one is usually fast even though the file is big,
    since only the changed blocks (recent rows) need to move."""
    _rsync("data/pricing/ohlcv_data.duckdb")


TASKS = {
    "intent": copy_intent,
    "exchange_state": copy_exchange_state,
    "db": copy_price_db,
}


def copy_all() -> None:
    for fn in TASKS.values():
        fn()


def main() -> None:
    valid = (*TASKS, "all")
    if len(sys.argv) != 2 or sys.argv[1] not in valid:
        print(f"usage: python -m scripts.copier {{{','.join(valid)}}}")
        sys.exit(1)
    task = sys.argv[1]
    (copy_all if task == "all" else TASKS[task])()


if __name__ == "__main__":
    main()
