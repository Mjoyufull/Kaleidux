#!/usr/bin/env python3
"""Map a condition from a local weather provider to a Kaleidux morph group."""

import argparse
import os
from pathlib import Path
import subprocess
import tomllib


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("condition", help="Condition reported by your weather provider")
    parser.add_argument("--mapping", type=Path, help="TOML [conditions] table")
    parser.add_argument("--output", "-o")
    parser.add_argument("--fps", default="30")
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
    layers = conditions.get(args.condition.strip().casefold())
    if not isinstance(layers, list) or not all(isinstance(layer, str) for layer in layers):
        parser.error(f"No valid layer array for {args.condition!r}")
    command = [os.environ.get("KLDCTL", "kldctl"), "morph", "--group", "weather", "--fps", args.fps]
    if args.output:
        command.extend(["--output", args.output])
    return subprocess.run([*command, "--", *layers], timeout=9).returncode


if __name__ == "__main__":
    raise SystemExit(main())
