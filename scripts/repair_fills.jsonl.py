# %%
import json

file_a = "logs/fills.jsonl"
file_b = "logs/fills_last2000.jsonl"
output_file = "logs/fills_new.jsonl"

fills = {}

# ---- Load A (dedupe automatically via dict) ----
with open(file_a, "r") as f:
    for line in f:
        if not line.strip():
            continue
        row = json.loads(line)
        fill_id = row["fill_id"]
        fills[fill_id] = row   # overwrites duplicates automatically

print(f"Loaded {len(fills)} unique fills from A")

# ---- Merge missing from B ----
added_from_b = 0

with open(file_b, "r") as f:
    for line in f:
        if not line.strip():
            continue
        row = json.loads(line)
        fill_id = row["fill_id"]

        if fill_id not in fills:
            fills[fill_id] = row
            added_from_b += 1

print(f"Added {added_from_b} missing fills from B")

# ---- Sort by timestamp (recommended for trading logs) ----
sorted_fills = sorted(
    fills.values(),
    key=lambda x: x["fill_timestamp_ms"]
)

# ---- Write final file ----
with open(output_file, "w") as f:
    for row in sorted_fills:
        f.write(json.dumps(row) + "\n")

print(f"Final file written: {output_file}")
print(f"Total rows: {len(sorted_fills)}")
# %%
