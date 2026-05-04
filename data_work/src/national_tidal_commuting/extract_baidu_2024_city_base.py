from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path

import pandas as pd
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[2]

CHAR_FIX = str.maketrans(
    {
        "⻄": "西",
        "⻓": "长",
        "⻬": "齐",
        "⻔": "门",
        "⻘": "青",
    }
)


def resolve_path(path_text: str | Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == "data_work" and len(path.parts) > 1:
        return (ROOT / Path(*path.parts[1:])).resolve()
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return (ROOT / path).resolve()


def clean_text(text: str) -> str:
    return unicodedata.normalize("NFKC", text).translate(CHAR_FIX)


def parse_congestion_page(text: str, prefix: str) -> pd.DataFrame:
    rows: list[dict] = []
    pattern = re.compile(r"^(\d+)([↑↓-])(\d+)(.+?)(\d+\.\d{3})([↑↓])(\d+\.\d+)%$")
    for raw in text.splitlines():
        compact = clean_text(raw).replace(" ", "")
        m = pattern.match(compact)
        if not m:
            continue
        rank, rank_dir, rank_change, city, index_value, yoy_dir, yoy_pct = m.groups()
        rows.append(
            {
                f"{prefix}_rank": int(rank),
                "city": city,
                f"{prefix}_rank_change_direction": rank_dir,
                f"{prefix}_rank_change": int(rank_change),
                f"{prefix}_congestion_index": float(index_value),
                f"{prefix}_index_yoy_direction": yoy_dir,
                f"{prefix}_index_yoy_pct": float(yoy_pct),
            }
        )
    out = pd.DataFrame(rows)
    if len(out) != 100:
        raise RuntimeError(f"Expected 100 rows for {prefix}, got {len(out)}.")
    return out.sort_values(f"{prefix}_rank").reset_index(drop=True)


def parse_commute_cost_page(text: str) -> pd.DataFrame:
    rows: list[dict] = []
    pattern = re.compile(r"^(\d+)(.+?)(\d+\.\d{2})(\d+\.\d{2})$")
    for raw in text.splitlines():
        compact = clean_text(raw).replace(" ", "")
        m = pattern.match(compact)
        if not m:
            continue
        rank, city, minutes, km = m.groups()
        rows.append(
            {
                "commute_cost_rank": int(rank),
                "city": city,
                "avg_commute_duration_min": float(minutes),
                "avg_commute_distance_km": float(km),
            }
        )
    out = pd.DataFrame(rows)
    if len(out) != 100:
        raise RuntimeError(f"Expected 100 commute-cost rows, got {len(out)}.")
    return out.sort_values("commute_cost_rank").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Baidu 2024 100-city indices from the annual traffic report PDF.")
    parser.add_argument(
        "--pdf",
        default="raw_data/baidu_2024_china_city_traffic_report.pdf",
        help="Downloaded Baidu 2024 annual city traffic report PDF.",
    )
    parser.add_argument(
        "--output-csv",
        default="raw_data/baidu_2024_city_congestion_base.csv",
        help="Merged city-base CSV output.",
    )
    parser.add_argument(
        "--output-xlsx",
        default="raw_data/baidu_2024_city_congestion_base.xlsx",
        help="Merged city-base Excel output.",
    )
    parser.add_argument(
        "--metadata-json",
        default="raw_data/baidu_2024_city_congestion_base_metadata.json",
        help="Source and extraction metadata JSON output.",
    )
    args = parser.parse_args()

    pdf_path = resolve_path(args.pdf)
    reader = PdfReader(str(pdf_path))
    page20 = clean_text(reader.pages[19].extract_text() or "")
    page21 = clean_text(reader.pages[20].extract_text() or "")
    page22 = clean_text(reader.pages[21].extract_text() or "")

    peak = parse_congestion_page(page20, "commute_peak")
    weekend = parse_congestion_page(page21, "weekend")
    commute = parse_commute_cost_page(page22)
    out = peak.merge(weekend, on="city", how="outer", validate="one_to_one").merge(
        commute, on="city", how="outer", validate="one_to_one"
    )
    out = out.sort_values("commute_peak_rank").reset_index(drop=True)
    out.insert(0, "source_year", 2024)
    out.insert(1, "source_report", "百度地图《2024年度中国城市交通报告》")
    out.insert(2, "source_pdf", str(pdf_path))
    out["city_for_dta_match"] = out["city"].astype(str).str.strip()
    if len(out) != 100 or out["city"].nunique() != 100:
        raise RuntimeError("Merged city base is not a complete 100-city table.")

    csv_path = resolve_path(args.output_csv)
    xlsx_path = resolve_path(args.output_xlsx)
    meta_path = resolve_path(args.metadata_json)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(csv_path, index=False)
    with pd.ExcelWriter(xlsx_path) as writer:
        out.to_excel(writer, sheet_name="city_base", index=False)
        peak.to_excel(writer, sheet_name="commute_peak", index=False)
        weekend.to_excel(writer, sheet_name="weekend", index=False)
        commute.to_excel(writer, sheet_name="commute_cost", index=False)

    metadata = {
        "source_report": "百度地图《2024年度中国城市交通报告》",
        "source_url": "https://renqi.map.baidu.com/reports/landing?id=180",
        "downloaded_pdf": str(pdf_path),
        "pages_used_1based": {
            "commute_peak_congestion": 20,
            "weekend_congestion": 21,
            "commute_cost": 22,
        },
        "primary_city_base_sort": "commute_peak_rank ascending",
        "primary_city_base_index": "commute_peak_congestion_index",
        "n_cities": int(len(out)),
        "outputs": {
            "csv": str(csv_path),
            "xlsx": str(xlsx_path),
        },
    }
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[extract_baidu_2024_city_base] wrote {len(out)} cities to {csv_path}")
    print(f"[extract_baidu_2024_city_base] wrote workbook to {xlsx_path}")
    print(f"[extract_baidu_2024_city_base] wrote metadata to {meta_path}")


if __name__ == "__main__":
    main()
