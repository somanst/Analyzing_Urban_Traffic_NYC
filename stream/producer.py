import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer

CSV_PATH = "../Automated_Traffic_Volume_Counts_20251227.csv"      # <-- change this
TOPIC = "traffic"
BOOTSTRAP = "localhost:9092"

# Simulated streaming speed:
PACE_SECONDS = 1  # 50ms between rows (set to 0 to send as fast as possible)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
    acks="all",
    retries=5,
)

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader):
        # Build a proper timestamp from Yr/M/D/HH/MM
        yr = safe_int(row.get("Yr"))
        m  = safe_int(row.get("M"))
        d  = safe_int(row.get("D"))
        hh = safe_int(row.get("HH"))
        mm = safe_int(row.get("MM"))

        # Add ISO timestamp field (string) for plotting
        try:
            ts = datetime(yr, m, d, hh, mm)
            row["ts_iso"] = ts.isoformat()
        except Exception:
            row["ts_iso"] = None

        # Convert Vol to number (CSV reads everything as strings)
        row["Vol"] = safe_float(row.get("Vol"), default=None)

        # Use SegmentID as key (keeps ordering per road segment)
        key = row.get("SegmentID") or i

        # (Optional) shrink message size by dropping geometry if you don't need it
        # row.pop("WktGeom", None)

        producer.send(TOPIC, key=key, value=row)

        if PACE_SECONDS > 0:
            time.sleep(PACE_SECONDS)

producer.flush()
producer.close()
print("✅ Finished streaming CSV rows to Kafka.")
