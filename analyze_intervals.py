
import sys
import re
from datetime import datetime

pattern = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}).*Scheduled next change for (\S+)")

last_time = {}

with open("kaleidux-daemon-2026-01-08_07-02-25.log", "r") as f:
    with open("intervals.txt", "w") as out:
        for line in f:
            match = pattern.search(line)
            if match:
                ts_str, output = match.groups()
                try:
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                    
                    if output in last_time:
                        delta = (ts - last_time[output]).total_seconds()
                        out.write(f"{output}: {delta:.3f}s\n")
                    
                    last_time[output] = ts
                except ValueError:
                    continue
