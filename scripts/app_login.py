import os
import sys
import pandas as pd
from datetime import date

from utils.config import paths
from utils.logging_utils import get_logger
from utils.db_utils import redshift_conn, run_query

logger = get_logger("app_login")

SQL = """
SELECT
   customer_id, dll.create_date, dll.source, dll.app_type 
FROM
   sttash_website_live.device_login_logs dll
WHERE
   customer_id IN (
   SELECT customer_id FROM sttash_website_live.collection_view)
   AND DATE(create_date) > :cutoff_date;
"""


def main():
    try:
        logger.info("Starting App Login extraction")

        cutoff = date(date.today().year, date.today().month, 1).replace(day=1)
        cutoff_str = cutoff.isoformat()
        logger.info("Cutoff date for this month: %s", cutoff_str)

        with redshift_conn() as conn:
            logger.info("Querying Redshift for login data...")
            res = run_query(conn, SQL, {"cutoff_date": cutoff_str})
            rows = res.fetchall()
            logger.info("Fetched %d rows", len(rows))

        if not rows:
            logger.warning("No rows returned from Redshift")
            df = pd.DataFrame(columns=["customer_id", "app_login", "latest_login_date"])
        else:
            df = pd.DataFrame(rows, columns=["customer_id", "create_date", "source", "app_type"]) \
                .assign(customer_id=lambda d: d["customer_id"].astype(str))

            # Aggregate to required 3 columns
            latest = df.sort_values("create_date").groupby("customer_id", as_index=False).tail(1)
            latest = latest[["customer_id", "create_date"]].rename(columns={"create_date": "latest_login_date"})
            latest["app_login"] = "Yes"

            # Include customers with no login as No? For only those in allocation. Load allocation ids if present
            alloc_path = os.path.join(paths.output_dir, "allocation_customer_ids.parquet")
            if os.path.exists(alloc_path):
                alloc = pd.read_parquet(alloc_path)
                alloc["customer_id"] = alloc["customer_id"].astype(str)
                out = alloc.merge(latest, on="customer_id", how="left")
                out["app_login"] = out["latest_login_date"].notna().map({True: "Yes", False: "No"})
            else:
                logger.warning("Allocation IDs file not found, output will include only customers with logins")
                out = latest

            out = out[["customer_id", "app_login", "latest_login_date"]]

        output_path = os.path.join(paths.output_dir, "app_login.parquet")
        df_out = df if df.empty else out
        df_out.to_parquet(output_path, index=False)
        logger.info("Wrote output: %s", output_path)
        logger.info("App Login extraction completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
