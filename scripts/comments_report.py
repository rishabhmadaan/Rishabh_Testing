import os
import sys
from datetime import date
import pandas as pd

from utils.config import paths
from utils.logging_utils import get_logger
from utils.db_utils import mysql_conn, run_query

logger = get_logger("comments_report")

# Contactable dispositions
CONTACTABLE = {
    'Paid',
    'Part Payment Collected',
    'Paid on call waiver pending',
    'PTP',
    'Call Back',
    'Future PTP',
    'Stop Calling',
    'RTP',
    'Dispute',
    'Customer seeking Waiver',
}

SQL = """
SELECT
    cc.customer_id,
    COALESCE(cdh.header, cs.name) AS collection_disposition,
    cd.disposition AS collection_sub_disposition,
    csd.sub_disposition AS collection_sub_disposition2,
    cc.callback_date,
    cc.ptp_date,
    cc.create_date AS comment_date
FROM
    collection_comment_data cc
LEFT JOIN st_comment sc 
    ON sc.id = cc.comment_id
LEFT JOIN st_customer_detail scd 
    ON scd.id = cc.customer_id
LEFT JOIN collection_status cs 
    ON sc.status_id = cs.id
LEFT JOIN collection_disposition_header cdh 
    ON cdh.id = cc.dis_head
LEFT JOIN collection_disposition cd 
    ON cd.id = cc.dis_body
LEFT JOIN collection_sub_disposition csd 
    ON csd.id = cc.dis_sub
WHERE
    cc.create_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
    AND cc.create_date < DATE_FORMAT(DATE_ADD(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01')
ORDER BY
    cc.create_date DESC;
"""

PRIORITY = [
    'Paid',
    'Part Payment Collected',
    'Paid on call waiver pending',
    'PTP',
    'Future PTP',
    'Customer seeking Waiver',
    'Dispute',
    'Call Back',
    'RTP',
    'Stop Calling',
    'Dialer Not Connected Calls',
    'Language Barrier',
    'Not Right Party',
    'Phone Not Picked',
    'Call Drop',
    'Home Address',
    'Mode of Tracing',
    'Reference 1',
    'Reference 2',
    'Not Contactable',
]


def priority_rank(value: str) -> int:
    try:
        return PRIORITY.index(value)
    except ValueError:
        return len(PRIORITY)


def main():
    try:
        logger.info("Starting Comments report")
        with mysql_conn() as conn:
            res = run_query(conn, SQL)
            rows = res.fetchall()
            logger.info("Fetched %d rows", len(rows))

        if not rows:
            out = pd.DataFrame(columns=[
                "customer_id", "yesterday_comment", "mtd_most_positive_comment",
                "contactable_vs_nc", "mtd_comment_count", "latest_ptp_date", "latest_callback_date"
            ])
        else:
            df = pd.DataFrame(rows, columns=[
                "customer_id","collection_disposition","collection_sub_disposition",
                "collection_sub_disposition2","callback_date","ptp_date","comment_date"
            ])
            df["customer_id"] = df["customer_id"].astype(str)
            for c in ["callback_date", "ptp_date", "comment_date"]:
                df[c] = pd.to_datetime(df[c], errors="coerce")

            # Yesterday's comment (latest from yesterday)
            today = pd.Timestamp.today().normalize()
            yesterday = today - pd.Timedelta(days=1)
            y_mask = (df["comment_date"] >= yesterday) & (df["comment_date"] < today)
            y_df = df[y_mask].sort_values("comment_date").groupby("customer_id", as_index=False).tail(1)
            y_df = y_df[["customer_id", "collection_disposition"]].rename(columns={"collection_disposition": "yesterday_comment"})

            # MTD most positive comment by priority order across dispositions (disposition OR sub_disposition)
            df["candidate"] = df["collection_sub_disposition"].fillna(df["collection_disposition"])
            df["rank"] = df["candidate"].apply(priority_rank)
            best = df.sort_values(["customer_id", "rank", "comment_date"]).groupby("customer_id", as_index=False).head(1)
            best = best[["customer_id", "candidate"]].rename(columns={"candidate": "mtd_most_positive_comment"})

            # Contactable vs NC - if any comment in CONTACTABLE
            contact = df.assign(is_contactable=lambda d: d["collection_sub_disposition"].isin(CONTACTABLE)) \
                        .groupby("customer_id")["is_contactable"].any().rename("contactable")
            contact = contact.reset_index()
            contact["contactable_vs_nc"] = contact["contactable"].map({True: "Contactable", False: "NC"})

            # Count of total comments this month
            counts = df.groupby("customer_id").size().rename("mtd_comment_count").reset_index()

            # Latest PTP and callback dates
            latest_dates = df.sort_values("comment_date").groupby("customer_id", as_index=False).tail(1)
            latest_dates = latest_dates[["customer_id", "ptp_date", "callback_date"]] \
                .rename(columns={"ptp_date": "latest_ptp_date", "callback_date": "latest_callback_date"})

            out = y_df.merge(best, on="customer_id", how="outer") \
                      .merge(contact[["customer_id", "contactable_vs_nc"]], on="customer_id", how="outer") \
                      .merge(counts, on="customer_id", how="outer") \
                      .merge(latest_dates, on="customer_id", how="outer")

            # Align with allocation
            alloc_path = os.path.join(paths.output_dir, "allocation_customer_ids.parquet")
            if os.path.exists(alloc_path):
                alloc = pd.read_parquet(alloc_path)
                out = alloc.merge(out, on="customer_id", how="left")

        output_path = os.path.join(paths.output_dir, "comments_report.parquet")
        out.to_parquet(output_path, index=False)
        logger.info("Wrote output: %s", output_path)
        logger.info("Comments report completed successfully")
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(3)


if __name__ == "__main__":
    main()
