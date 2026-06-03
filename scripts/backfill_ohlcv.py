import sys
sys.path.insert(0, '.')

import src.ingestion.hyperliquid as hl
from datetime import datetime, timezone

hl._init()

since_ms = int(datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp() * 1000)
hl.get_start_time = lambda symbol: since_ms

hl.run_ohlcv_dl()
hl.update_daily()
hl.update_latest_view()