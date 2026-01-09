
import sys
import re
from datetime import datetime

# Define pattern for timestamp and outcome
pattern = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*Scheduled next change for (\S+)")

last_time = {}

try:
    with open("kaleidux-daemon-2026-01-08_07-02-25.log", "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                ts_str, output = match.groups()
                try:
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                    
                    if output in last_time:
                        delta = (ts - last_time[output]).total_seconds()
                        if delta < 0.2:
                            print(f"BURST DETECTED for {output}: {delta:.3f}s at {ts_str}")
                    
                    last_time[output] = ts
                except ValueError:
                    continue
except Exception as e:
    print(f"Error: {e}")
