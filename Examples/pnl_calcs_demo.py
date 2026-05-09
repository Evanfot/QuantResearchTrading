import pandas as pd

data = pd.read_json('logs/fills.jsonl',lines=True)

import datetime as dt
day_start = dt.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000

prev_data = data[data['fill_timestamp_ms']<=day_start]
today_data = data[data['fill_timestamp_ms']>day_start]

from src.positions.position_rebuilder import PositionRebuilder
mids = pd.read_csv('data/snapshots/mids.csv').set_index('symbol')['mid']
day_open = pd.read_csv('data/snapshots/day_open.csv').set_index('symbol')['mid']
day_port = PositionRebuilder()
prev_port = PositionRebuilder()
total_port = PositionRebuilder()
day_positions = day_port.rebuild_from_fills(today_data.to_dict(orient="records"))
prev_positions = prev_port.rebuild_from_fills(prev_data.to_dict(orient='records'))
total = total_port.rebuild_from_fills(data.to_dict(orient='records'))

for key, item in day_positions.items():
    day_positions[key]['mid'] = mids.at[key]
    day_positions[key]['day_pnl'] = day_positions[key]['qty']*(mids.at[key] - day_positions[key]['avg_price'])
    day_positions[key]['unrealised_pnl'] = day_positions[key]['qty']*(mids.at[key] - day_positions[key]['avg_price'])

for key, item in prev_positions.items():
    prev_positions[key]['mid'] = mids.at[key]
    prev_positions[key]['unrealised_pnl'] = prev_positions[key]['qty']*(mids.at[key] - prev_positions[key]['avg_price'])
    prev_positions[key]['day_pnl'] = prev_positions[key]['qty']*(mids.at[key] - day_open.at[key])

for key in total.keys():
    mid = mids.at[key]
    open_px = day_open.at[key]
    prev_qty = prev_positions.get(key, {}).get('qty', 0)
    day_qty = day_positions.get(key, {}).get('qty', 0)
    day_avg = day_positions[key]['avg_price'] if key in day_positions else 0

    if prev_qty != 0 and day_qty != 0 and prev_qty * day_qty < 0:
        # Opposite signs: day trades partially or fully close the prev position.
        # The closed portion is priced at day_avg (realized); remainder marks to mid.
        offset_qty = min(abs(prev_qty), abs(day_qty))
        prev_sign = 1 if prev_qty > 0 else -1
        day_sign = 1 if day_qty > 0 else -1
        realized = prev_sign * offset_qty * (day_avg - open_px)
        rem_prev = prev_qty + day_sign * offset_qty
        rem_day = day_qty + prev_sign * offset_qty
        total[key]['day_pnl'] = realized + rem_prev * (mid - open_px) + rem_day * (mid - day_avg)
    else:
        total[key]['day_pnl'] = prev_qty * (mid - open_px) + day_qty * (mid - day_avg)

    total[key]['unrealised_pnl'] = total[key]['qty'] * (mid - total[key]['avg_price'])

