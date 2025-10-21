import os
import sys
import pandas as pd

from utils.config import paths
from utils.logging_utils import get_logger

logger = get_logger("compile_master")

FILES = {
    "allocation": "allocation_customer_ids.parquet",
    "app_login": "app_login.parquet",
    "tickets": "tickets_data.parquet",
    "payments": "payments_data.parquet",
    "comments": "comments_report.parquet",
    "ivr": "ivr_data.parquet",
}


def load(name: str) -> pd.DataFrame:
    path = os.path.join(paths.output_dir, FILES[name])
    if not os.path.exists(path):
        logger.warning("File missing: %s", path)
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "customer_id" not in df.columns:
        logger.warning("customer_id missing in %s", name)
        return pd.DataFrame()
    df["customer_id"] = df["customer_id"].astype(str)
    return df


def main():
    try:
        logger.info("Starting master compilation")
        alloc = load("allocation")
        if alloc.empty:
            logger.error("Allocation base is missing; cannot compile master")
            sys.exit(1)
        master = alloc.copy()

        for key in ["app_login", "tickets", "payments", "comments", "ivr"]:
            df = load(key)
            if df.empty:
                logger.warning("Skipping missing/empty %s", key)
                continue
            # Avoid duplicate columns on merge
            dupes = [c for c in df.columns if c != "customer_id" and c in master.columns]
            df = df.drop(columns=dupes)
            master = master.merge(df, on="customer_id", how="left")
            logger.info("Merged %s; master now has %d rows and %d cols", key, master.shape[0], master.shape[1])

        output_path = os.path.join(paths.output_dir, "master_compiled.parquet")
        master.to_parquet(output_path, index=False)
        logger.info("Wrote master output: %s", output_path)
        logger.info("Master compilation completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
