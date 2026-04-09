import argparse
import re
from pathlib import Path

import pandas as pd


CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def detect_encoding(input_path: str) -> str:
    """
    Try several common Chinese encodings and pick the one that looks plausible.
    This is mainly to avoid garbled Chinese columns.
    """
    candidate_encodings = ["utf-8-sig", "utf-8", "gb18030", "gbk", "cp936"]
    usecols = ["roadname", "semantic"]

    for enc in candidate_encodings:
        try:
            df = pd.read_csv(
                input_path,
                usecols=usecols,
                nrows=200,
                encoding=enc,
                engine="python",
            )
            text = " ".join(df["roadname"].astype(str).tolist() + df["semantic"].astype(str).tolist())
            if CJK_RE.search(text):
                return enc
        except Exception:
            continue

    return "utf-8"


def extract_unique_roads(
    input_path: str,
    chunksize: int = 200_000,
):
    required_cols = ["roadseg_id", "roadname", "semantic", "location", "geometry"]

    encoding = detect_encoding(input_path)

    seen_road_ids = set()
    records = []

    for chunk in pd.read_csv(
        input_path,
        usecols=required_cols,
        chunksize=chunksize,
        encoding=encoding,
        engine="python",
    ):
        chunk = chunk.rename(columns={"roadseg_id": "road_id"})

        # Deduplicate inside a chunk first (reduces iterations)
        chunk = chunk.drop_duplicates(subset=["road_id"])

        for row in chunk.itertuples(index=False):
            road_id = row.road_id
            if road_id in seen_road_ids:
                continue
            seen_road_ids.add(road_id)
            records.append(
                {
                    "road_id": road_id,
                    "roadname": row.roadname,
                    "semantic": row.semantic,
                    "location": row.location,
                    "geometry": row.geometry,
                }
            )

    out_df = pd.DataFrame(
        records,
        columns=["road_id", "roadname", "semantic", "location", "geometry"],
    )
    return out_df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV path, e.g. B:\\\\RA工作\\\\SpeedNow\\\\Raw_data\\\\speed_Beijing_all_wgs84.csv",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "Processed_Data" / "road_list.xlsx"),
        help="Output Excel path (default: New_Strategy/Processed_Data/road_list.xlsx)",
    )
    parser.add_argument("--chunksize", type=int, default=200_000)
    args = parser.parse_args()

    input_path = args.input
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_out = extract_unique_roads(input_path, chunksize=args.chunksize)

    try:
        df_out.to_excel(output_path, index=False)
    except Exception as e:
        # If Excel export fails (rare), fallback to CSV
        fallback = output_path.with_suffix(".csv")
        df_out.to_csv(fallback, index=False, encoding="utf-8-sig")
        print(f"Excel export failed: {e}. Fallback to CSV: {fallback}")


if __name__ == "__main__":
    main()

