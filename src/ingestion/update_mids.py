# %%
import pandas as pd
from datetime import datetime, UTC
import duckdb
import os

from pathlib import Path
import json
import hashlib
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL

# === CONFIGURATION ===
from dotenv import load_dotenv
load_dotenv()

# db_dir = os.getenv('DB_DIR')
db_dir = f'data'
db_path = f'{db_dir}/pricing/mids.duckdb'
meta_folder = f'{db_dir}/meta'
latest_view_folder = f'{db_dir}/snapshots'
TABLE_NAME = f"hyperliquid_1m"

def get_all_ltps(info):
    mids = info.all_mids()
    now = datetime.now(UTC)
    rows = []

    for symbol, price in mids.items():
        rows.append({
            "symbol": symbol,
            "mid":price,
            "downloaded_at": now,
        })
    df = pd.DataFrame(rows)

    return df

def update_latest_view(file):
    file.to_csv(f"{latest_view_folder}/mids.csv",index=False)

def save_mapping_to_folder(mapping, folder_path):
    """Saves the mapping dictionary to a JSON file with a timestamp in the history folder."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  
    
    # Generate a timestamped filename
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f'mapping_{timestamp}.json'
    file_path = os.path.join(folder_path, filename)
    
    with open(file_path, 'w') as f:
        json.dump(mapping, f, indent=4)
    print(f"Mapping saved to folder: {file_path}")


def load_latest_mapping(folder_path):
    """Loads the most recent mapping dictionary from the history folder."""
    if not os.path.exists(folder_path):
        return None
    # Get the list of all files in the history folder
    files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    if not files:
        return None  # No history files found
    # Load the most recent file
    # Sort so that first element is most recent file
    files.sort(reverse=True)
    latest_file = os.path.join(folder_path, files[0])
    with open(latest_file, 'r') as f:
        return json.load(f)

def has_mapping_changed(current_mapping, saved_mapping):
    """Checks if the current mapping has changed compared to the saved one."""
    # Generate a hash of the dictionary to easily compare
    current_hash = hashlib.md5(json.dumps(current_mapping, sort_keys=True).encode('utf-8')).hexdigest()
    if saved_mapping is None:
        return True
    saved_hash = hashlib.md5(json.dumps(saved_mapping, sort_keys=True).encode('utf-8')).hexdigest()
    return current_hash != saved_hash

def init_duckdb_table(db_path):
    #Not used, just a reminder of the schema
    conn = duckdb.connect(db_path)
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        symbol VARCHAR,
        mid DOUBLE,
        downloaded_at TIMESTAMP,
        PRIMARY KEY(symbol, downloaded_at)
    )
    """)
    conn.close()

def insert_into_duckdb(db_path, mids):
    conn = duckdb.connect(db_path)
    conn.execute(f"INSERT OR IGNORE INTO {TABLE_NAME} SELECT * FROM mids")
    conn.close()

def run_update_mids():
    logfile = Path(f"{db_dir}/logs/mids_cron.log")
    info = Info(MAINNET_API_URL, skip_ws=True)
    mids = get_all_ltps(info)
    init_duckdb_table(db_path)
    insert_into_duckdb(db_path, mids)
    with logfile.open("a") as f:
        f.write(f"{datetime.now(UTC)}-INFO:DB Updated\n")
    update_latest_view(mids)
    # Try to load the most recent mapping from the history folder
    name_to_coin = info.name_to_coin
    saved_mapping = load_latest_mapping(meta_folder)

    # Check if the mapping has changed, or if it's the first time
    if has_mapping_changed(name_to_coin, saved_mapping):
        with logfile.open("a") as f:
            f.write(f"{datetime.now(UTC)}-INFO:New mapping saved\n")
        save_mapping_to_folder(name_to_coin, meta_folder)
    return mids
if __name__ == "__main__":
    run_update_mids()