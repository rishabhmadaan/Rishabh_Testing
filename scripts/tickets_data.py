import os
import sys
from datetime import date, datetime
import pandas as pd

from utils.config import paths
from utils.logging_utils import get_logger
from utils.db_utils import mysql_conn, run_query
from utils.date_utils import months_between, bucket_months

logger = get_logger("tickets_data")

SQL = """
select user_id, source, date(create_date) as create_date
from ts_tickets tt
where date(create_date) > '2024-01-01';
"""


def main():
    try:
        logger.info("Starting Tickets Data extraction")
        with mysql_conn() as conn:
            res = run_query(conn, SQL)
            rows = res.fetchall()
            logger.info("Fetched %d rows", len(rows))

        if not rows:
            logger.warning("No tickets returned")
            out = pd.DataFrame(columns=["customer_id", "latest_ticket_source", "latest_ticket_recency_bucket"])
        else:
            df = pd.DataFrame(rows, columns=["user_id", "source", "create_date"]) \
                .assign(customer_id=lambda d: d["user_id"].astype(str))
            df["create_date"] = pd.to_datetime(df["create_date"]).dt.date

            latest = df.sort_values("create_date").groupby("customer_id", as_index=False).tail(1)
            today = date.today()
            latest["months_old"] = latest["create_date"].apply(lambda d: months_between(d, today))
            latest["latest_ticket_recency_bucket"] = latest["months_old"].apply(bucket_months)
            latest = latest.rename(columns={"source": "latest_ticket_source"})
            out = latest[["customer_id", "latest_ticket_source", "latest_ticket_recency_bucket"]]

            # Align with allocation if present
            alloc_path = os.path.join(paths.output_dir, "allocation_customer_ids.parquet")
            if os.path.exists(alloc_path):
                alloc = pd.read_parquet(alloc_path)
                out = alloc.merge(out, on="customer_id", how="left")

        output_path = os.path.join(paths.output_dir, "tickets_data.parquet")
        out.to_parquet(output_path, index=False)
        logger.info("Wrote output: %s", output_path)
        logger.info("Tickets Data extraction completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
