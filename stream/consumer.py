import csv
import json
import os
from datetime import datetime
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:9092"
TOPIC = "traffic"

# Set to None to accept all SegmentIDs
TARGET_SEGMENT_ID = None   # e.g. "12345"

SAVE_TO_CSV = True
OUT_CSV = "received_messages.csv"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id="receiver-v1",
    auto_offset_reset="earliest",   # use "latest" to only read new messages
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

fieldnames = [
    "received_at", "ts_iso", "Vol", "SegmentID", "Boro", "Direction",
    "street", "fromSt", "toSt", "RequestID", "Yr", "M", "D", "HH", "MM"
]

writer = None
f = None
if SAVE_TO_CSV:
    write_header = not os.path.exists(OUT_CSV) or os.path.getsize(OUT_CSV) == 0
    f = open(OUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()
        f.flush()

def matches_segment(msg):
    if TARGET_SEGMENT_ID is None:
        return True
    return str(msg.get("SegmentID")) == str(TARGET_SEGMENT_ID)

def format_time(msg):
    # Prefer ts_iso if exists
    if msg.get("ts_iso"):
        return msg.get("ts_iso")
    # Fallback to split fields
    yr = msg.get("Yr")
    m = msg.get("M")
    d = msg.get("D")
    hh = msg.get("HH")
    mm = msg.get("MM")
    return f"{yr}-{m}-{d} {hh}:{mm}"

print(f"✅ Listening on topic '{TOPIC}' ... (CTRL+C to stop)")
if TARGET_SEGMENT_ID is not None:
    print(f"🔎 Filtering to SegmentID={TARGET_SEGMENT_ID}")

try:
    for record in consumer:
        msg = record.value

        if not matches_segment(msg):
            continue

        received_at = datetime.now().isoformat(timespec="seconds")
        time_str = format_time(msg)

        seg = msg.get("SegmentID")
        vol = msg.get("Vol")

        # Print a "message received" line
        print(f"[RECEIVED {received_at}] SegmentID={seg} Vol={vol} Time={time_str}")

        # Uncomment to print full message:
        # print(json.dumps(msg, indent=2, ensure_ascii=False))

        if SAVE_TO_CSV and writer:
            writer.writerow({
                "received_at": received_at,
                "ts_iso": msg.get("ts_iso"),
                "Vol": vol,
                "SegmentID": seg,
                "Boro": msg.get("Boro"),
                "Direction": msg.get("Direction"),
                "street": msg.get("street"),
                "fromSt": msg.get("fromSt"),
                "toSt": msg.get("toSt"),
                "RequestID": msg.get("RequestID"),
                "Yr": msg.get("Yr"),
                "M": msg.get("M"),
                "D": msg.get("D"),
                "HH": msg.get("HH"),
                "MM": msg.get("MM"),
            })
            f.flush()

except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")
finally:
    if f:
        f.close()
    consumer.close()
