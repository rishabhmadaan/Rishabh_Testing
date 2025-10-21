import os
import sys
from datetime import date
import pandas as pd

from utils.config import paths
from utils.logging_utils import get_logger
from utils.date_utils import month_date_range

logger = get_logger("ivr_data")

EXPECTED_COLS = [
    "mobileNumber","CustomerID","campaignName","leadName","attemptNum","startDate","answerDate","endDate",
    "callDuration","billSeconds","creditsUsed","disposition","hangupCause","hangupCode","clid","dtmfTime",
    "voiceFileName","circle","operator","digitsPressed","circuitId","slaveId"
]


def read_ivr_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl")
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    return df


def main():
    try:
        logger.info("Starting IVR data aggregation")
        ivr_dir = paths.payments_dir  # user said IVR dump is in payments dir
        if not os.path.isdir(ivr_dir):
            logger.error("IVR dir not found: %s", ivr_dir)
            sys.exit(1)

        files = [
            os.path.join(ivr_dir, f)
            for f in os.listdir(ivr_dir)
            if not f.startswith(".") and os.path.isfile(os.path.join(ivr_dir, f))
        ]
        logger.info("Found %d IVR files", len(files))

        frames = []
        for fp in files:
            try:
                df = read_ivr_file(fp)
                # standardize columns if possible
                lower_map = {c.lower(): c for c in df.columns}
                for c in EXPECTED_COLS:
                    if c not in df.columns and c.lower() in lower_map:
                        df.rename(columns={lower_map[c.lower()]: c}, inplace=True)
                # filter to current month rows via startDate or answerDate
                for dc in ["startDate", "answerDate", "endDate"]:
                    if dc in df.columns:
                        df[dc] = pd.to_datetime(df[dc], errors="coerce")
                frames.append(df)
            except Exception as e:
                logger.exception("Failed reading IVR file %s: %s", fp, e)

        if not frames:
            logger.warning("No IVR data found")
            out = pd.DataFrame(columns=["customer_id"])  # will add daily AI/CI later if present
        else:
            allv = pd.concat(frames, ignore_index=True)
            if "CustomerID" not in allv.columns:
                logger.error("CustomerID column missing in IVR files")
                sys.exit(2)
            allv["customer_id"] = allv["CustomerID"].astype(str).str.strip()

            # Determine date per row: prefer answerDate, else startDate
            date_col = None
            if "answerDate" in allv.columns:
                date_col = "answerDate"
            elif "startDate" in allv.columns:
                date_col = "startDate"
            elif "endDate" in allv.columns:
                date_col = "endDate"
            
            if date_col is None:
                logger.error("No date column found in IVR data")
                sys.exit(3)

            allv["date_only"] = pd.to_datetime(allv[date_col], errors="coerce").dt.date

            # current month filter
            month_start = date(date.today().year, date.today().month, 1)
            month_days = month_date_range(date.today())
            allv = allv[allv["date_only"].isin(month_days)]

            # AI = count of rows per customer_id per date
            ai = allv.groupby(["customer_id", "date_only"]).size().rename("AI").reset_index()
            # CI = count where disposition == ANSWERED
            if "disposition" in allv.columns:
                ci = allv[allv["disposition"].astype(str).str.upper().eq("ANSWERED")] \
                        .groupby(["customer_id", "date_only"]).size().rename("CI").reset_index()
            else:
                ci = ai.assign(CI=0)[["customer_id", "date_only", "CI"]].copy()

            # Pivot to columns per date with suffixes
            ai_pivot = ai.pivot(index="customer_id", columns="date_only", values="AI").fillna(0).astype(int)
            ci_pivot = ci.pivot(index="customer_id", columns="date_only", values="CI").fillna(0).astype(int)

            # Rename columns with suffix
            ai_pivot.columns = [f"{d.isoformat()}_AI" for d in ai_pivot.columns]
            ci_pivot.columns = [f"{d.isoformat()}_CI" for d in ci_pivot.columns]

            # Combine and compute MTD sums
            out = ai_pivot.join(ci_pivot, how="outer").reset_index()
            ai_cols = [c for c in out.columns if c.endswith("_AI")]
            ci_cols = [c for c in out.columns if c.endswith("_CI")]
            out["MTD_AI"] = out[ai_cols].sum(axis=1)
            out["MTD_CI"] = out[ci_cols].sum(axis=1)

            # Align with allocation
            alloc_path = os.path.join(paths.output_dir, "allocation_customer_ids.parquet")
            if os.path.exists(alloc_path):
                alloc = pd.read_parquet(alloc_path)
                out = alloc.merge(out, left_on="customer_id", right_on="customer_id", how="left")

        output_path = os.path.join(paths.output_dir, "ivr_data.parquet")
        out.to_parquet(output_path, index=False)
        logger.info("Wrote output: %s", output_path)
        logger.info("IVR data aggregation completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
