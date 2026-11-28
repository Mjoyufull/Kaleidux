#!/usr/bin/env python3
"""Map a condition from a local weather provider to a Kaleidux morph group."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tomllib
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen


CONDITIONS = {
    "clear": [],
    "cloudy": ["fog"],
    "drizzle": ["rain"],
    "fog": ["fog"],
    "light rain": ["rain"],
    "rain": ["rain"],
    "snow": ["snow"],
    "thunderstorm": ["rain", "lightning"],
}

WMO = {
    **dict.fromkeys((0, 1), "clear"),
    **dict.fromkeys((2, 3), "cloudy"),
    **dict.fromkeys((45, 48), "fog"),
    **dict.fromkeys((51, 53, 55, 56, 57), "drizzle"),
    **dict.fromkeys((61, 63, 65, 66, 67, 80, 81, 82), "rain"),
    **dict.fromkeys((71, 73, 75, 77, 85, 86), "snow"),
    **dict.fromkeys((95, 96, 97, 99), "thunderstorm"),
}


def live_condition(url):
    with urlopen(url, timeout=8) as response:
        payload = json.loads(response.read(65536))
    current = payload.get("current") if isinstance(payload, dict) else None
    code = current.get("weather_code") if isinstance(current, dict) else None
    if type(code) is not int or code not in WMO:
        raise ValueError("Forecast did not contain a supported current weather code")
    return WMO[code]


def frame_rate(value):
    if value.casefold() == "unlimited":
        return 65535
    try:
        rate = int(value)
    except ValueError:
        rate = -1
    if not 0 <= rate <= 65535:
        raise argparse.ArgumentTypeError("Use 0, a frame rate up to 65535, or unlimited")
    return rate


def apply_condition(kldctl, output, fps, profiles, condition):
    layers = profiles.get(condition.strip().casefold())
    if not isinstance(layers, list) or not all(isinstance(layer, str) for layer in layers):
        raise ValueError(f"No valid layer array for {condition!r}")
    outputs = json.loads(subprocess.check_output([kldctl, "layers", *output], timeout=9))
    expected = (layers, fps) if layers else ([], 0)
    if outputs and all(
        next(((group["layers"], group["fps"]) for group in target.get("morph_groups", [])
              if group["name"] == "weather"), ([], 0)) == expected
        for target in outputs
    ):
        return
    subprocess.run([kldctl, "morph", "--group", "weather", "--fps", str(fps),
                    *output, "--", *layers], check=True, timeout=9)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("condition", nargs="?", help="Condition reported by your weather provider")
    parser.add_argument("--location", nargs=2, type=float, metavar=("LAT", "LON"),
                        help="Fetch live conditions from Open-Meteo")
    parser.add_argument("--source", help="Use a provider URL returning Open-Meteo weather JSON")
    parser.add_argument("--interval", type=int, default=1800, help="Seconds between live updates")
    parser.add_argument("--once", action="store_true", help="Apply one live update")
    parser.add_argument("--mapping", type=Path, help="TOML [conditions] table")
    parser.add_argument("--output", "-o")
    parser.add_argument("--fps", type=frame_rate, default=30)
    args = parser.parse_args()
    conditions = CONDITIONS
    if args.mapping:
        try:
            with args.mapping.open("rb") as file:
                conditions = tomllib.load(file)["conditions"]
        except (OSError, KeyError, ValueError) as error:
            parser.error(str(error))
    if not isinstance(conditions, dict):
        parser.error("Use a [conditions] table with an array of layer names for each condition")
    if sum(option is not None for option in (args.condition, args.location, args.source)) != 1:
        parser.error("Choose a condition, --location LAT LON, or --source URL")
    if args.interval < 1:
        parser.error("Use a positive --interval")
    source = args.source
    if args.location:
        latitude, longitude = args.location
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            parser.error("Invalid latitude or longitude")
        query = urlencode({"latitude": latitude, "longitude": longitude, "current": "weather_code"})
        source = f"https://api.open-meteo.com/v1/forecast?{query}"
    if source and urlsplit(source).scheme not in ("http", "https"):
        parser.error("Weather sources must use HTTP or HTTPS")
    kldctl = os.environ.get("KLDCTL", "kldctl")
    output = ["--output", args.output] if args.output else []
    try:
        while True:
            try:
                condition = args.condition if args.condition is not None else live_condition(source)
                apply_condition(kldctl, output, args.fps, conditions, condition)
            except (KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
                print(f"weather-morph: {error}", file=sys.stderr)
                if args.once or not source:
                    return 1
            if args.once or not source:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
