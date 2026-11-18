#!/usr/bin/env python3
"""Add measurement gates and road-region overlays to Figure 1 screenshots."""

from pathlib import Path

import cv2

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_random_detection_audit_v1"
OUTPUT = ROOT / "flowvedio/batch_outputs/20260723_24_state_dependent_recognition_v5/analysis_basic_v1/outbound_rdit_collaborator_report_v1/detection_examples_annotated"
OUTPUT.mkdir(parents=True, exist_ok=True)

FONT_PATHS = [
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/STHeiti Medium.ttc",
    "/Library/Fonts/Arial Unicode.ttf",
]
FONT_PATH = next((Path(p) for p in FONT_PATHS if Path(p).exists()), None)


EXAMPLES = [
    {
        "source": "sample_01_20260723_145835_P06.jpg",
        "output": "sample_01_annotated.jpg",
        "near": (440, 800, 950, 1600),
        "far": (700, 600, 920, 1200),
        "labels": ["北向区域", "南向区域"],
    },
    {
        "source": "sample_04_20260723_163424_P10.jpg",
        "output": "sample_04_annotated.jpg",
        "near": (500, 800, 1100, 1300, 1900),
        "far": (850, 540, 1080, 1150, 1320),
        "labels": ["固定北向", "潮汐车道", "固定南向"],
    },
    {
        "source": "sample_06_20260724_153535_C12.jpg",
        "output": "sample_06_annotated.jpg",
        "near": (380, 400, 700, 800, 1200),
        "far": (480, 300, 680, 750, 900),
        "labels": ["固定北向", "潮汐车道", "固定南向"],
    },
    {
        "source": "sample_08_20260724_211307_C20.jpg",
        "output": "sample_08_annotated.jpg",
        "near": (240, 500, 620, 820, 1200),
        "far": (500, 300, 640, 700, 810),
        "labels": ["固定北向", "潮汐车道", "固定南向"],
    },
]

COLORS = [
    (40, 130, 190, 68),
    (245, 185, 45, 92),
    (65, 160, 95, 68),
]


def font(size):
    if FONT_PATH:
        return ImageFont.truetype(str(FONT_PATH), size=size)
    return ImageFont.load_default()


def add_label(draw, xy, text, fill, text_font):
    box = draw.textbbox(xy, text, font=text_font, anchor="mm")
    pad = 5
    draw.rounded_rectangle(
        (box[0] - pad, box[1] - pad, box[2] + pad, box[3] + pad),
        radius=5, fill=(0, 0, 0, 185), outline=fill[:3] + (255,), width=2,
    )
    draw.text(xy, text, font=text_font, fill=(255, 255, 255, 255), anchor="mm")


for item in EXAMPLES:
    image = Image.open(SOURCE / item["source"]).convert("RGBA")
    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    scale = image.width / 1920 if image.width > 1280 else image.width / 1280
    line_width = max(4, round(6 * scale))
    text_font = font(max(18, round(25 * scale)))

    near, far = item["near"], item["far"]
    y1, y2 = near[1], far[1]
    near_x = [near[0], *near[2:]]
    far_x = [far[0], *far[2:]]
    gate_color = (220, 45, 45, 255)
    draw.line((near_x[0], y1, near_x[-1], y1), fill=gate_color, width=line_width)
    add_label(draw, ((near_x[0] + near_x[-1]) / 2, y1 + 24 * scale), "Near gate", gate_color, text_font)
    draw.line((far_x[0], y2, far_x[-1], y2), fill=(255, 255, 255, 225), width=max(2, line_width // 2))

    composed = Image.alpha_composite(image, overlay).convert("RGB")
    composed.save(OUTPUT / item["output"], quality=94, subsampling=0)


raw_frame = OUTPUT / "region_definition_frame.jpg"
capture = cv2.VideoCapture(str(ROOT / "flowvedio/2026072324彩田路/IMG_0017.MOV"))
capture.set(cv2.CAP_PROP_POS_MSEC, 2150.6 * 1000)
ok, frame = capture.read()
capture.release()
if not ok:
    raise RuntimeError("Could not extract region-definition frame")
cv2.imwrite(str(raw_frame), frame)
image = Image.open(raw_frame).convert("RGBA")
overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
draw = ImageDraw.Draw(overlay)
text_font = font(28)
small_font = font(23)
near = (500, 800, 1100, 1300, 1900)
far = (850, 540, 1080, 1150, 1320)
near_x = [near[0], *near[2:]]
far_x = [far[0], *far[2:]]
labels = ["固定北向区域", "潮汐可变区域", "固定南向区域"]
subtitles = ["始终向北", "方向随状态切换", "始终向南"]
for idx, (label, subtitle) in enumerate(zip(labels, subtitles)):
    polygon = [
        (near_x[idx], near[1]), (near_x[idx + 1], near[1]),
        (far_x[idx + 1], far[1]), (far_x[idx], far[1]),
    ]
    color = COLORS[idx]
    draw.polygon(polygon, fill=color, outline=color[:3] + (245,))
    cx = sum(p[0] for p in polygon) / 4
    cy = sum(p[1] for p in polygon) / 4
    add_label(draw, (cx, cy - 22), label, color, text_font)
    add_label(draw, (cx, cy + 20), subtitle, color, small_font)

draw.line((near_x[0], near[1], near_x[-1], near[1]), fill=(220, 45, 45, 255), width=7)
add_label(draw, ((near_x[0] + near_x[-1]) / 2, near[1] + 30), "近端计数线", (220, 45, 45, 255), text_font)
draw.line((far_x[0], far[1], far_x[-1], far[1]), fill=(255, 255, 255, 235), width=4)

def arrow(start, end, color):
    draw.line((*start, *end), fill=color, width=11)
    vx, vy = end[0] - start[0], end[1] - start[1]
    length = max(1.0, (vx * vx + vy * vy) ** 0.5)
    ux, uy = vx / length, vy / length
    px, py = -uy, ux
    size = 25
    base = (end[0] - ux * size, end[1] - uy * size)
    draw.polygon(
        [end, (base[0] + px * size * .55, base[1] + py * size * .55),
         (base[0] - px * size * .55, base[1] - py * size * .55)],
        fill=color,
    )

arrow((780, 740), (930, 575), (255, 255, 255, 245))
arrow((1175, 740), (1115, 575), (255, 255, 255, 245))
arrow((1430, 575), (1610, 740), (255, 255, 255, 245))
Image.alpha_composite(image, overlay).convert("RGB").save(
    OUTPUT / "region_definition_annotated.jpg", quality=95, subsampling=0
)
