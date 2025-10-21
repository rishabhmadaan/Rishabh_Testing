import os
import sys
from datetime import date
import pandas as pd
import numpy as np

from utils.config import paths
from utils.logging_utils import get_logger

logger = get_logger("payments_data")

EXPECTED_COLS = {
    "customer_id": "customer_id",
    "amt_payment": "amt_payment",
    "DATE(op.create_date)": "create_date",
    "transaction_id": "transaction_id",
    "mode": "mode",
    "MONTHNAME(create_date)": "month_name",
    "YEAR(create_date)": "year",
    "presentation_status": "presentation_status",
    "bounce_reason": "bounce_reason",
    "payment_channel": "payment_channel",
    "payment_from_customer": "payment_from_customer",
    "DATE(op.received_date)": "received_date",
    "remarks": "remarks",
}


def read_payment_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl")
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    colmap = {}
    lower_cols = {c.lower(): c for c in df.columns}
    for raw, norm in EXPECTED_COLS.items():
        key = raw.lower()
        if key in lower_cols:
            colmap[lower_cols[key]] = norm
        else:
            # try simplified names
            simplified = raw.lower().replace("(op.", "(").replace("date(", "").replace(")", "")
            for c in df.columns:
                if simplified in c.lower().replace(" ", ""):
                    colmap[c] = norm
                    break
    df = df.rename(columns=colmap)
    return df


def main():
    try:
        logger.info("Starting Payments aggregation")
        month_start = date(date.today().year, date.today().month, 1)
        logger.info("Current month start: %s", month_start)

        payments_dir = paths.payments_dir
        if not os.path.isdir(payments_dir):
            logger.error("Payments dir not found: %s", payments_dir)
            sys.exit(1)

        files = [
            os.path.join(payments_dir, f)
            for f in os.listdir(payments_dir)
            if not f.startswith(".") and os.path.isfile(os.path.join(payments_dir, f))
        ]
        logger.info("Found %d payment files", len(files))

        frames = []
        for fp in files:
            try:
                logger.info("Reading payments file: %s", fp)
                df = read_payment_file(fp)
                df = normalize_columns(df)
                if "customer_id" not in df.columns:
                    logger.warning("customer_id column missing in %s, skipping", fp)
                    continue
                # parse dates
                for c in ["create_date", "received_date"]:
                    if c in df.columns:
                        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
                frames.append(df)
            except Exception as e:
                logger.exception("Failed to read %s: %s", fp, e)

        if not frames:
            logger.warning("No valid payments data found")
            out = pd.DataFrame(columns=["customer_id", "sum_paid_this_month", "latest_payment_date", "payment_count_this_month"])
        else:
            allp = pd.concat(frames, ignore_index=True)
            allp["customer_id"] = allp["customer_id"].astype(str).str.strip()

            # Filter to current month based on create_date or received_date
            def is_current_month(d: date) -> bool:
                return d is not None and d >= month_start and d <= date.today()

            current_mask = False
            if "create_date" in allp.columns:
                current_mask = current_mask | allp["create_date"].apply(lambda d: is_current_month(d))
            if "received_date" in allp.columns:
                current_mask = current_mask | allp["received_date"].apply(lambda d: is_current_month(d))

            current = allp[current_mask] if isinstance(current_mask, pd.Series) else allp.iloc[0:0]

            sums = current.groupby("customer_id")["amt_payment"].sum(min_count=1).rename("sum_paid_this_month")
            counts = current.groupby("customer_id").size().rename("payment_count_this_month")

            # latest payment date overall
            date_cols = [c for c in ["create_date", "received_date"] if c in allp.columns]
            if date_cols:
                allp["latest_payment_date"] = allp[date_cols].apply(lambda row: max([d for d in row if pd.notna(d)]), axis=1)
                latest = allp.sort_values("latest_payment_date").groupby("customer_id", as_index=False).tail(1)
                latest = latest[["customer_id", "latest_payment_date"]]
            else:
                latest = pd.DataFrame(columns=["customer_id", "latest_payment_date"])            

            out = (
                latest.set_index("customer_id").join(sums, how="outer").join(counts, how="outer").reset_index()
            )

            # Align with allocation
            alloc_path = os.path.join(paths.output_dir, "allocation_customer_ids.parquet")
            if os.path.exists(alloc_path):
                alloc = pd.read_parquet(alloc_path)
                out = alloc.merge(out, on="customer_id", how="left")

        output_path = os.path.join(paths.output_dir, "payments_data.parquet")
        out.to_parquet(output_path, index=False)
        logger.info("Wrote output: %s", output_path)
        logger.info("Payments aggregation completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
