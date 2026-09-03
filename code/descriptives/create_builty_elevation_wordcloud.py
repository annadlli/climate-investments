"""
Authors: Anna Li
Date: 2026-09-01

Word cloud of the screened Builty elevation descriptions, for showing the
keyword filter caught what it was meant to.

One row per property. clean_builty.do collapses description with (firstnm), so
this is the earliest permit at each address, not every permit.

Run by hand; write the PNG to output/figures/builty_elevation_wordcloud.png.
"""

from __future__ import annotations

import argparse
import math
import random
import re
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

# Function words and legal boilerplate that carry no information about the work.
STOPWORDS = set(
    """
    a an and are as at be been by for from had has have in into is it its of on
    onto or per that the this to under was were will with all herewith
    """.split()
)

# Collapse inflections so one idea doesn't split across three words.
NORMALIZE = {
    "bib": "Build It Back",  # NYC Build It Back recovery program
    "elevated": "elevate", "elevating": "elevate", "elevations": "elevation",
    "raised": "raise", "raises": "raise", "raising": "raise",
    "jacked": "jack", "jacking": "jack",          # "jacking up the house"
    "houses": "house", "homes": "home", "dwellings": "dwelling",
    "foundations": "foundation", "pilings": "piling", "floors": "floor",
    "feet": "foot", "flooded": "flood", "flooding": "flood",
    "remodeled": "remodel", "remodeling": "remodel",
}

# Words that make a description unambiguously about raising a structure.
ELEVATION = r"elevat|raise|raising|raised|jack|lift|bfe|base flood|freeboard|piling|pier|stilt"

#some distinct colors to use
COLORS = ("#17365D", "#2F75B5", "#00A6A6", "#70AD47", "#ED7D31", "#8064A2")

WIDTH, HEIGHT, TOP = 1800, 1100, 20


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, help="Project data root")
    p.add_argument("--output", required=True, help="Output PNG")
    p.add_argument("--max-words", type=int, default=65)
    return p.parse_args()


def font(size: int, bold: bool = False):
    name = "Arial Bold.ttf" if bold else "Arial.ttf"
    for path in (f"/System/Library/Fonts/Supplemental/{name}",
                 f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf"):
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    raise FileNotFoundError("no Arial or DejaVu Sans")


def word_counts(text: pd.Series, max_words: int) -> list[tuple[str, int]]:
    # Every word in every description, counted. 3+ letters, so numbers and "ft"  drop out
    counts: Counter[str] = Counter()
    for raw in re.findall(r"[a-z][a-z'-]{2,}", " ".join(text).lower()):
        word = NORMALIZE.get(raw.strip("-"), raw.strip("-"))
        if word not in STOPWORDS:
            counts[word] += 1
    return counts.most_common(max_words)

# Draws the picture. Biggest words first, each one dropped into whatever gap is still free, shrinking it if nothing fits.
def draw_cloud(ranked, output: Path) -> None:
    # Blank white page with a small outer margin.
    image = Image.new("RGB", (WIDTH, HEIGHT), "white")

    # A map of the page, one cell per pixel: True once something is drawn there.
    # Only covers below the title, so words can never run into the header.
    taken = np.zeros((HEIGHT - TOP, WIDTH), dtype=bool)

    rng = random.Random(3390)

    # ranked is sorted, so the first count is the largest and the last the smallest.
    high, low = ranked[0][1], ranked[-1][1]

    for i, (word, count) in enumerate(ranked):
        # Font size from the count, on a log scale
        scale = (math.log(count) - math.log(low)) / (math.log(high) - math.log(low))
        size = int(27 + scale * 112)
        placed = False

        # Try at that size. If there is no gap for it, knock 3px off and try  again, down to 22px. A crowded page just gets smaller words, not a crash.
        while size >= 22 and not placed:
            # Top 12 in bold so the headline words carry.
            f = font(size, bold=i < 12)

            box = f.getbbox(word)
            layer = Image.new("RGBA", (box[2] - box[0] + 12, box[3] - box[1] + 12),
                              (255, 255, 255, 0))
            # Colors just cycle by rank; they carry no meaning.
            ImageDraw.Draw(layer).text((6 - box[0], 6 - box[1]), word, font=f, fill=COLORS[i % 6])

            # A few of the smaller words go sideways, purely so the page looks less like a list.
            if i > 12 and rng.random() < 0.14:
                layer = layer.rotate(90, expand=True)


            ink = np.asarray(layer.getchannel("A")) > 0
            h, w = ink.shape

            if h > HEIGHT - TOP or w > WIDTH:
                size -= 3
                continue

            # Try reproducible random positions. A small margin prevents words
            # from visually touching even when their actual glyphs do not.
            margin = 7
            for _ in range(3500):
                x = rng.randrange(0, WIDTH - w + 1)
                y = rng.randrange(0, HEIGHT - TOP - h + 1)
                y0, y1 = max(0, y - margin), min(HEIGHT - TOP, y + h + margin)
                x0, x1 = max(0, x - margin), min(WIDTH, x + w + margin)
                if not taken[y0:y1, x0:x1].any():
                    image.paste(layer, (x, TOP + y), layer)
                    taken[y:y + h, x:x + w] |= ink
                    placed = True
                    break

            if not placed:
                size -= 3

    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output, dpi=(180, 180))


def main() -> None:
    args = parse_args()
    frame = pd.read_stata(Path(args.data) / "clean" / "builty_elevations.dta",
                          convert_categoricals=False)
    text = frame["description"].fillna("").astype(str)

    # The headline number: how many descriptions actually say "elevate".
    n_elevated = int(text.str.lower().str.contains(ELEVATION, regex=True).sum())

    ranked = word_counts(text, args.max_words)
    draw_cloud(ranked, Path(args.output))

    print(f"Saved {args.output}")
    print(f"Elevation language: {n_elevated:,}/{len(frame):,} ({n_elevated / len(frame):.1%})")
    print("Top words:", ranked[:15])


if __name__ == "__main__":
    main()
