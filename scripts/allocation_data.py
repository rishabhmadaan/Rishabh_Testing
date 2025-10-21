import os
import sys
import pandas as pd

from utils.config import paths
from utils.logging_utils import get_logger

logger = get_logger("allocation_data")


def main():
    try:
        logger.info("Starting Allocation Data extraction")
        file_path = paths.allocation_file
        logger.info("Reading allocation file: %s", file_path)

        if not os.path.exists(file_path):
            logger.error("Allocation file not found: %s", file_path)
            sys.exit(1)

        df = pd.read_excel(file_path, engine="openpyxl")
        logger.info("Allocation file loaded with %d rows and %d columns", df.shape[0], df.shape[1])

        # Robust column detection
        possible_cols = [
            "customer_id",
            "customerID",
            "CustomerID",
            "Customer_ID",
            "Customer Id",
            "Customer Id ",
            "Customer Ids",
        ]
        found_col = None
        for c in possible_cols:
            if c in df.columns:
                found_col = c
                break
        if found_col is None:
            # try case-insensitive
            lower_map = {c.lower(): c for c in df.columns}
            for c in ["customer_id", "customerid", "customer id"]:
                if c in lower_map:
                    found_col = lower_map[c]
                    break
        if found_col is None:
            logger.error("Could not find customer_id column in allocation file. Columns: %s", list(df.columns))
            sys.exit(2)

        out = (
            df[[found_col]].rename(columns={found_col: "customer_id"})
            .dropna()
            .drop_duplicates()
        )
        out["customer_id"] = out["customer_id"].astype(str).str.strip()
        logger.info("Extracted %d unique customer_ids", out.shape[0])

        output_path = os.path.join(paths.output_dir, "allocation_customer_ids.parquet")
        out.to_parquet(output_path, index=False)
        logger.info("Wrote output: %s", output_path)
        logger.info("Allocation Data extraction completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
