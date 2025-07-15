import datetime as dt

from .formatting import _d_round, fmt_percentage
from datequarter import DateQuarter as dq
from decimal import Decimal

def _calcualte_business_days(month_start_date: dt.date, end_date: dt.date) -> dict:
    """
    Gets the number of business days in a given month.
    """

    business_days = 0
    for day in range(month_start_date.day, end_date.day+1):
        date = dt.date(month_start_date.year, month_start_date.month, day)
        if date.weekday() < 5:
            business_days += 1
    return business_days

def generate_date_inputs(date: dt.date) -> dict[str:dict]:
    """
    Returns dictionary of DateTime dates and DateQuarter quarters for use in query and tranformation inputs
    """

    cq = dq.from_date(date)

    cm_start_date = date.replace(day=1)
    cm_end_date = (date.replace(day=28) + dt.timedelta(days=4)).replace(day=1)- dt.timedelta(days=1)

    total_business_days = _calcualte_business_days(cm_start_date,cm_end_date)
    mtd_business_days = _calcualte_business_days(cm_start_date,date)

    try:
        mtd_business_days_percent = _d_round(Decimal(mtd_business_days / total_business_days),6)
    except ZeroDivisionError:
        mtd_business_days = Decimal(0)
    
    return {
        "date": {
            "cw_start_date": date - dt.timedelta(days=date.weekday() + 1),
            "cw_end_date": date + dt.timedelta(days=(6 - date.weekday()) - 1),
            "cm_start_date": cm_start_date,
            "cm_end_date": cm_end_date,
            "cm_name": date.strftime("%B"),
            "cq_start_date": cq.start_date(),
            "cq_end_date": cq.end_date(),
            "cy_start_date": date.replace(month=1, day=1),
            "cy_end_date": date.replace(month=12, day=31),
        },
        "month": {
            "total_business_days": total_business_days,
            "mtd_business_days": mtd_business_days,
            "mtd_business_days_percent": mtd_business_days_percent
        },
        "quarter": {
            "cq": cq,
            "cq_minus_1": cq - 1,
            "cq_minus_2": cq - 2,
            "cq_minus_3": cq - 3,
            "cq_minus_4": cq - 4,
        },
    }