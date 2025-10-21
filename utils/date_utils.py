from datetime import date, datetime, timedelta
from typing import List


def month_date_range(target_date: date) -> List[date]:
    start = date(target_date.year, target_date.month, 1)
    if target_date.month == 12:
        next_month = date(target_date.year + 1, 1, 1)
    else:
        next_month = date(target_date.year, target_date.month + 1, 1)
    days = (next_month - start).days
    return [start + timedelta(days=i) for i in range(days)]


def months_between(d1: date, d2: date) -> int:
    if d1 is None or d2 is None:
        return 10**6
    if d1 > d2:
        d1, d2 = d2, d1
    years = d2.year - d1.year
    months = years * 12 + (d2.month - d1.month)
    return months


def bucket_months(months: int) -> str:
    if months <= 1:
        return "<=1 month"
    if 1 < months <= 3:
        return "1-3 months"
    if 3 < months <= 6:
        return "3-6 months"
    if 6 < months <= 12:
        return "6-12 months"
    return ">12 months"
