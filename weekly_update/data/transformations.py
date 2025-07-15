import datetime as dt
import numpy as np
import logging
import pandas as pd
import pytz

from decimal import Decimal
from enum import IntEnum
from .formatting import _d_round

DM_FIELD = "DM"
DATE_FIELDS = ["CLOSEDATE", "STAGE_1_DATE"]

log = logging.getLogger(__name__)

class Category(IntEnum):
    STAGE_1 = 1
    PIPELINE = 2
    BOOKED = 3


class Metric(IntEnum):
    DM = 1
    COUNT = 2


def standardize_data(df: pd.DataFrame):
    if isinstance(df.at[0, DM_FIELD], int) or isinstance(df.at[0, DM_FIELD], float):
        df[DM_FIELD] = df[DM_FIELD].apply(_float_to_decimal)

    try:
        assert isinstance(df.at[0, DM_FIELD], Decimal)
    except AssertionError:
        raise TypeError(
            f"Field {DM_FIELD} has type {type(df.at[0, DM_FIELD])} instead of Decimal"
        )

    for col in DATE_FIELDS:
        if isinstance(df.at[0, col], pd.Timestamp):
            df[col] = df[col].apply(lambda x: x.date())
        if isinstance(df.at[0, col], str):
            df[col] = df[col].apply(
                lambda x: dt.date.fromisoformat(x) if pd.notnull(x) else x
            )

    log.debug(
        "Loaded dataframe, size: {} by {}; {}".format(*df.shape, df.memory_usage())
    )
    # df.to_csv(f"test_data_export.csv")

    return df


def load_from_csv(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path, parse_dates=DATE_FIELDS)

    return standardize_data(df=df)


def salesforce_dict_to_dataframe(raw_data: dict) -> pd.DataFrame:
    LA_TIMEZONE = pytz.timezone("America/Los_Angeles")

    data = pd.DataFrame(
        [
            dict(
                NAME=row["Name"],
                STAGENAME=row["StageName"],
                FORECAST_CATEGORY=None,
                SF_FCST=row["ForecastCategoryName"],
                COMMS_VS_IDENTITY=row["Comms_vs_Identity__c"],
                REGION=row["Sales_Team_Region__c"],
                CLOSEDATE=row["CloseDate"],
                DM=row["Amount_Direct_Margin__c"],
                CREATED_DATE=dt.datetime.fromisoformat(
                    f"{row['CreatedDate'][0:-5]}+00:00"
                )
                .astimezone(tz=LA_TIMEZONE)
                .date(),
                SAO_DATE=row["SAO_Date__c"],
            )
            for row in raw_data["records"]
        ]
    )

    def _set_fcst_cat(val):
        if val in ("Closed-Lost", "Closed-Won"):
            return "Won"

    data["FORECAST_CATEGORY"] = data["STAGENAME"].apply(_set_fcst_cat)
    data["FORECAST_CATEGORY"] = np.where(
        data["FORECAST_CATEGORY"].isna(), data["SF_FCST"], data["FORECAST_CATEGORY"]
    )

    data.drop(columns=["SF_FCST"])

    data["STAGE_1_DATE"] = pd.to_datetime(
        data["SAO_DATE"], errors="coerce", format="ISO8601"
    ).dt.date

    return standardize_data(data)


def _float_to_decimal(num) -> Decimal:
    if not isinstance(num, float) and not isinstance(num, int):
        raise TypeError(f"Value {num} is type {type(num)}. Should be int or float")
    return Decimal(num).quantize(Decimal("1.00"), rounding="ROUND_HALF_EVEN")


def _decimal_to_float(num: Decimal) -> float:
    return float(num)


def _fcst_opps(data: pd.DataFrame, forecast_category: str) -> pd.DataFrame:
    return data[(data["FORECAST_CATEGORY"] == forecast_category)]


def _stage_opps(data: pd.DataFrame, stagename: str) -> pd.DataFrame:
    return data[(data["STAGENAME"] == stagename)]


def _booked_opps(data: pd.DataFrame) -> pd.DataFrame:
    return _fcst_opps(data, "Won")


def _open_opps(data: pd.DataFrame) -> pd.DataFrame:
    return data[
        (data["FORECAST_CATEGORY"] != "Ommitted") & (data["FORECAST_CATEGORY"] != "Won")
    ]


def _top_n_opps(data: pd.DataFrame, number: int = 3) -> list:
    sorted_data = data.sort_values(by="DM", ascending=False, ignore_index=True)
    assert sorted_data.shape[0] > 0
    top_records = []
    for n in range(0, number):
        try:
            top_records.append((sorted_data.at[n, "NAME"], sorted_data.at[n, "DM"]))
        except KeyError:
            top_records.append(("", 0))
    return top_records


def stage_1_in_period(
    data: pd.DataFrame,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:

    return data[
        (data["STAGE_1_DATE"] >= start_date) & (data["STAGE_1_DATE"] <= end_date)
    ]


def closedate_in_period(
    data: pd.DataFrame,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:

    return data[(data["CLOSEDATE"] >= start_date) & (data["CLOSEDATE"] <= end_date)]


def closing_in_period(
    data: pd.DataFrame,
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:

    open_opps = _open_opps(data=data)

    return closedate_in_period(data=open_opps, start_date=start_date, end_date=end_date)


def booked_in_period(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> pd.DataFrame:
    booked_opps = _booked_opps(data=data)

    return closedate_in_period(
        data=booked_opps, start_date=start_date, end_date=end_date
    )


def total_in_period(
    data: pd.DataFrame,
    category: Category,
    metric: Metric,
    start_date: dt.date,
    end_date: dt.date,
) -> Decimal:

    match category:
        case Category.STAGE_1:
            category_data = stage_1_in_period(
                data=data, start_date=start_date, end_date=end_date
            )
        case Category.PIPELINE:
            category_data = closing_in_period(
                data=data, start_date=start_date, end_date=end_date
            )
        case Category.BOOKED:
            category_data = booked_in_period(
                data=data, start_date=start_date, end_date=end_date
            )

    match metric:
        case Metric.DM:
            return category_data["DM"].sum()
        case Metric.COUNT:
            return category_data["NAME"].count()


def top_opps_in_period(
    data: pd.DataFrame, category: Category, start_date: dt.date, end_date: dt.date
) -> list:

    match category:
        case Category.STAGE_1:
            category_data = stage_1_in_period(
                data=data, start_date=start_date, end_date=end_date
            )
        case Category.PIPELINE:
            category_data = closing_in_period(
                data=data, start_date=start_date, end_date=end_date
            )
        case Category.BOOKED:
            category_data = booked_in_period(
                data=data, start_date=start_date, end_date=end_date
            )

    return _top_n_opps((category_data))


def top_commits_in_period(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date, exclude: list = []
) -> list:
    commit_opps = _fcst_opps(data, "Commit")
    period_commit_opps = closedate_in_period(
        data=commit_opps, start_date=start_date, end_date=end_date
    )

    if exclude:
        opps_to_exclude = [opp for opp, _ in exclude]
        period_commit_opps = period_commit_opps[
            ~period_commit_opps["NAME"].isin(opps_to_exclude)
        ]

    return _top_n_opps(data=period_commit_opps)


def top_best_case_in_period(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> list:
    bc_opps = _fcst_opps(data, "Best Case")
    bc_opps = closedate_in_period(
        data=bc_opps, start_date=start_date, end_date=end_date
    )
    return _top_n_opps(data=bc_opps)


def top_business_terms_in_period(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> list:
    bt_opps = _stage_opps(data, "Business Terms")
    bt_opps = closedate_in_period(
        data=bt_opps, start_date=start_date, end_date=end_date
    )
    return _top_n_opps(data=bt_opps)


def bookings_by_region(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> dict:
    booked_ytd = booked_in_period(data=data, start_date=start_date, end_date=end_date)

    dm_by_region = booked_ytd[["REGION", "DM"]].groupby(["REGION"]).sum()

    dm_by_region["PERCENT"] = dm_by_region["DM"] / dm_by_region["DM"].sum()
    dm_by_region_dict = dm_by_region.to_dict()
    dm_by_region_dict["DM"]["Total"] = dm_by_region["DM"].sum()
    return dm_by_region_dict


def bookings_by_comms_vs_identity(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> dict:
    booked_ytd = booked_in_period(data=data, start_date=start_date, end_date=end_date)

    dm_by_comms_vs_identity = (
        booked_ytd[["COMMS_VS_IDENTITY", "DM"]].groupby(["COMMS_VS_IDENTITY"]).sum()
    )

    dm_by_comms_vs_identity["PERCENT"] = (
        dm_by_comms_vs_identity["DM"] / dm_by_comms_vs_identity["DM"].sum()
    )

    return dm_by_comms_vs_identity.to_dict()


def calculate_gap_coverage(
    management_call: Decimal,
    won_dm: Decimal,
    commit_dm: Decimal,
    bc_dm: Decimal,
    pipeline_dm: Decimal,
):
    # log.debug(
    #     "Gap Calc Called, inputs: mgmg_call = {}, won_dm = {}, commit_dm = {}, bc_dm = {}, pipeline_dm = {}".format(
    #         management_call, won_dm, commit_dm, bc_dm, pipeline_dm
    #     ),
    # )
    print("Gap Calc Called, inputs: mgmg_call = {}, won_dm = {}, commit_dm = {}, bc_dm = {}, pipeline_dm = {}".format(
            management_call, won_dm, commit_dm, bc_dm, pipeline_dm))
    
    weighted_pipe = (
        (commit_dm * Decimal(0.85))
        + (bc_dm * Decimal(0.30))
        + (pipeline_dm * Decimal(0.15))
    )

    gap = management_call - won_dm

    coverage = _d_round(weighted_pipe / gap, 2)
    
    gap_data = {
        "Weighted Pipe": weighted_pipe,
        "DM Gap": gap,
        "Coverage": coverage,
    }

    # log.debug("Gap Calc output: {}".format(gap_data))
    print("Gap Calc output: {}".format(gap_data))

    return gap_data


def pipeline_by_forecast(
    data: pd.DataFrame, management_call: Decimal, start_date: dt.date, end_date: dt.date
) -> dict[str:dict]:
    closing_in_q = closedate_in_period(
        data=data,
        start_date=start_date,
        end_date=end_date,
    )

    pipe_by_forecast_category: pd.DataFrame = (
        closing_in_q[["FORECAST_CATEGORY", "DM"]].groupby(["FORECAST_CATEGORY"]).sum()
    )

    fcst_dict = pipe_by_forecast_category.to_dict()

    if management_call > 0:
        gap_coverage = calculate_gap_coverage(
            management_call=management_call,
            won_dm=pipe_by_forecast_category.at["Won", "DM"],
            commit_dm=pipe_by_forecast_category.at["Commit", "DM"],
            bc_dm=pipe_by_forecast_category.at["Best Case", "DM"],
            pipeline_dm=pipe_by_forecast_category.at["Pipeline", "DM"],
        )
        fcst_dict["Management Call"] = gap_coverage

    return fcst_dict


def pipeline_by_comms_vs_identity(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> dict:
    created_ytd = stage_1_in_period(data=data, start_date=start_date, end_date=end_date)

    dm_by_comms_vs_identity = (
        created_ytd[["COMMS_VS_IDENTITY", "DM"]].groupby(["COMMS_VS_IDENTITY"]).sum()
    )

    dm_by_comms_vs_identity["PERCENT"] = (
        dm_by_comms_vs_identity["DM"] / dm_by_comms_vs_identity["DM"].sum()
    )

    return dm_by_comms_vs_identity.to_dict()


def pipeline_by_region(
    data: pd.DataFrame, start_date: dt.date, end_date: dt.date
) -> dict:
    created_ytd = stage_1_in_period(data=data, start_date=start_date, end_date=end_date)

    dm_by_region = created_ytd[["REGION", "DM"]].groupby(["REGION"]).sum()

    dm_by_region["PERCENT"] = dm_by_region["DM"] / dm_by_region["DM"].sum()

    dm_by_region_dict = dm_by_region.to_dict()

    return dm_by_region_dict
