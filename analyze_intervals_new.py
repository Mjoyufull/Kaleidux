
import sys
import re
from datetime import datetime

if len(sys.argv) < 2:
    print("Usage: python analyze_intervals_new.py <log_file>")
    sys.exit(1)

log_file = sys.argv[1]

pattern = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*Scheduled next change for (\S+)")

last_time = {}

with open(log_file, "r") as f:
    with open("intervals_new.txt", "w") as out:
        for line in f:
            match = pattern.search(line)
            if match:
                ts_str, output = match.groups()
                try:
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                    
                    if output in last_time:
                        delta = (ts - last_time[output]).total_seconds()
                        out.write(f"{output}: {delta:.3f}s at {ts_str}\n")
                    
                    last_time[output] = ts
                except ValueError:
                    continue
