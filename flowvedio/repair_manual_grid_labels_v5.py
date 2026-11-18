#!/usr/bin/env python3
"""Redraw coordinate labels on the M01-M18 manual review grids."""

import argparse
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
GRID_DIR = (
    ROOT
    / "batch_outputs"
    / "20260723_24_state_dependent_manual_review_v5"
    / "reference_grids"
)


def load_font(size: int) -> ImageFont.ImageFont:
    candidates = [
        Path("/System/Library/Fonts/Supplemental/Arial Bold.ttf"),
        Path("/System/Library/Fonts/PingFang.ttc"),
    ]
    for candidate in candidates:
        if candidate.exists():
            try:
                return ImageFont.truetype(str(candidate), size)
            except OSError:
                continue
    return ImageFont.load_default()


def label_box(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.ImageFont,
) -> None:
    x, y = xy
    bbox = draw.textbbox((x, y), text, font=font, stroke_width=1)
    draw.rectangle(
        (bbox[0] - 3, bbox[1] - 2, bbox[2] + 3, bbox[3] + 2),
        fill=(0, 0, 0),
    )
    draw.text(
        (x, y),
        text,
        font=font,
        fill=(255, 255, 0),
        stroke_width=1,
        stroke_fill=(0, 0, 0),
    )


def repair(path: Path) -> None:
    image = Image.open(path).convert("RGB")
    draw = ImageDraw.Draw(image)
    width, height = image.size
    coordinate_font = load_font(18 if width >= 1900 else 15)

    # Keep the original image dimensions and coordinate origin unchanged.
    # Labels sit just below the title strip and inside the left edge.
    x_label_y = 62 if height >= 1000 else 58
    for x in range(0, width, 100):
        label_box(draw, (x + 4, x_label_y), str(x), coordinate_font)

    for y in range(100, height, 100):
        label_box(draw, (6, y + 4), str(y), coordinate_font)

    image.save(path, quality=95)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contact-only", action="store_true")
    args = parser.parse_args()
    paths = sorted(GRID_DIR.glob("M*_grid.jpg"))
    if len(paths) != 18:
        raise RuntimeError(f"Expected 18 grid images, found {len(paths)}")
    if not args.contact_only:
        for path in paths:
            repair(path)
            print(path)

    previews = []
    for path in paths:
        image = Image.open(path).convert("RGB")
        image.thumbnail((640, 360))
        tile = Image.new("RGB", (640, 360), (245, 245, 245))
        tile.paste(image, ((640 - image.width) // 2, (360 - image.height) // 2))
        previews.append(tile)
    sheet = Image.new("RGB", (1280, 360 * 9), (245, 245, 245))
    for index, preview in enumerate(previews):
        sheet.paste(preview, ((index % 2) * 640, (index // 2) * 360))
    sheet.save(GRID_DIR.parent / "three_region_reference_contact_sheet.jpg", quality=95)


if __name__ == "__main__":
    main()
