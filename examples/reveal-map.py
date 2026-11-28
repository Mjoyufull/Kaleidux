#!/usr/bin/env python3
"""Write a Kaleidux reveal PNG from a subject mask or seeded colonies."""

import argparse
import math
import random

from PIL import Image


def distances(targets, width, height):
    """Approximate distance to a target with an eight-neighbour chamfer sweep."""
    field = [0.0 if target else math.inf for target in targets]
    for y in range(height):
        for x in range(width):
            index = y * width + x
            if x:
                field[index] = min(field[index], field[index - 1] + 1)
            if y:
                field[index] = min(field[index], field[index - width] + 1)
                if x:
                    field[index] = min(field[index], field[index - width - 1] + math.sqrt(2))
                if x + 1 < width:
                    field[index] = min(field[index], field[index - width + 1] + math.sqrt(2))
    for y in reversed(range(height)):
        for x in reversed(range(width)):
            index = y * width + x
            if x + 1 < width:
                field[index] = min(field[index], field[index + 1] + 1)
            if y + 1 < height:
                field[index] = min(field[index], field[index + width] + 1)
                if x:
                    field[index] = min(field[index], field[index + width - 1] + math.sqrt(2))
                if x + 1 < width:
                    field[index] = min(field[index], field[index + width + 1] + math.sqrt(2))
    return field


def outline(mask_path, size):
    with Image.open(mask_path) as image:
        # Accept a grayscale mask, or the alpha channel of a transparent cutout.
        mask = image.getchannel("A") if "A" in image.getbands() else image.convert("L")
        mask.thumbnail((size, size), Image.Resampling.BILINEAR)
    width, height = mask.size
    subject = [pixel >= 128 for pixel in mask.tobytes()]
    if not any(subject) or all(subject):
        raise ValueError("The mask needs both subject and background pixels")
    inside = distances((not pixel for pixel in subject), width, height)
    outside = distances(subject, width, height)
    inside_max = max(inside)
    outside_max = max(outside)
    pixels = []
    for index, is_subject in enumerate(subject):
        if is_subject and inside[index] < 2:
            # Follow the silhouette from top to bottom before filling it.
            timing = 0.06 + 0.20 * (index // width) / height
            accent = 255
        elif is_subject:
            timing = 0.29 + 0.28 * inside[index] / inside_max
            accent = 0
        else:
            timing = 0.60 + 0.38 * outside[index] / outside_max
            accent = 0
        pixels.append((round(timing * 255), accent, 0))
    field = Image.new("RGB", mask.size)
    field.putdata(pixels)
    return field


def contagion(size, aspect, seed):
    width = max(1, round(size * min(1, aspect)))
    height = max(1, round(size * min(1, 1 / aspect)))
    rng = random.Random(seed)
    origins = [(rng.uniform(0, width), rng.uniform(0, height), rng.random() * size * 0.20)
               for _ in range(7)]
    noise = Image.frombytes("L", (22, 22), bytes(rng.randrange(256) for _ in range(22 * 22)))
    noise = noise.resize((width, height), Image.Resampling.BILINEAR)
    values = []
    for index, jitter in enumerate(noise.tobytes()):
        y, x = divmod(index, width)
        arrival = min(math.hypot(x - sx, y - sy) + delay for sx, sy, delay in origins)
        values.append(arrival + jitter * 0.16 * size / 255)
    low, high = min(values), max(values)
    field = Image.new("RGB", (width, height))
    field.putdata([(round(255 * (value - low) / max(high - low, 1)), 130, 0) for value in values])
    return field


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    outline_args = modes.add_parser("outline", help="Reveal outward from a subject silhouette")
    outline_args.add_argument("mask", help="Grayscale mask or transparent subject PNG")
    contagion_args = modes.add_parser("contagion", help="Reveal from scattered colonies")
    contagion_args.add_argument("--aspect", type=float, default=16 / 9, help="Source width / height")
    contagion_args.add_argument("--seed", type=int, default=1)
    for mode in (outline_args, contagion_args):
        mode.add_argument("output")
        mode.add_argument("--size", type=int, default=256, help="Maximum field edge in pixels")
    args = parser.parse_args()
    if not 16 <= args.size <= 1024:
        parser.error("--size must be between 16 and 1024")
    if args.mode == "contagion" and not (0.05 <= args.aspect <= 20):
        parser.error("--aspect must be between 0.05 and 20")
    try:
        field = (outline(args.mask, args.size) if args.mode == "outline"
                 else contagion(args.size, args.aspect, args.seed))
        field.save(args.output, format="PNG")
    except (OSError, ValueError) as error:
        parser.exit(1, f"reveal-map: {error}\n")


if __name__ == "__main__":
    main()
