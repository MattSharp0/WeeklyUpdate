import datetime as dt
import logging
import pandas as pd

from decimal import Decimal

from .date_values import generate_date_inputs
from .formatting import fmt_percentage, fmt_currency
from .transformations import (
    Category,
    Metric,
    bookings_by_region,
    bookings_by_comms_vs_identity,
    pipeline_by_forecast,
    top_opps_in_period,
    top_commits_in_period,
    top_best_case_in_period,
    top_business_terms_in_period,
    total_in_period,
    pipeline_by_comms_vs_identity,
    pipeline_by_region,
)


def generate_weekly_update_dict(
    data: pd.DataFrame,
    management_call: Decimal,
    monthly_pipe_target: Decimal,
    quarterly_booking_target: int | None = None,
    for_date: dt.date = dt.date.today(),
) -> dict:
    """
    Generates dictionary of weekly update data\n
    Parameters:\n
    data: a Pandas DataFrame\n
    management_call: a non-zero Decimal. If 0, does not include management call data\n
    [Optional] quarterly_target: a non-zero integer. Defaults to management_call\n
    [Optional] for_date: a Datetime.Date. Defaults to Datetime.Date.Today\n
    """

    if not quarterly_booking_target:
        quarterly_booking_target = management_call


    date_values = generate_date_inputs(date=for_date)
    
    cq_stage_1_opp_dm = total_in_period(
                data=data,
                category=Category.STAGE_1,
                metric=Metric.DM,
                start_date=date_values["date"]["cq_start_date"],
                end_date=date_values["date"]["cq_end_date"],
            )

    month_of_quarter = date_values["date"]["cm_start_date"].month - date_values["date"]["cq_start_date"].month

    try:
        qtd_pipeline_attainment = fmt_percentage(cq_stage_1_opp_dm / (monthly_pipe_target*month_of_quarter+monthly_pipe_target*date_values["month"]["mtd_business_days_percent"]))
    except ZeroDivisionError:
        qtd_pipeline_attainment = fmt_percentage(Decimal(0))

    ytd_regional_bookings = bookings_by_region(
        data=data, start_date=date_values["date"]["cy_start_date"], end_date=for_date
    )
    ytd_comms_vs_id_bookings = bookings_by_comms_vs_identity(
        data=data, start_date=date_values["date"]["cy_start_date"], end_date=for_date
    )
    cq_forecast = pipeline_by_forecast(
        data=data,
        management_call=management_call,
        start_date=date_values["date"]["cq_start_date"],
        end_date=date_values["date"]["cq_end_date"],
    )
    cq_comms_vs_id_bookings = bookings_by_comms_vs_identity(
        data=data, start_date=date_values["date"]["cq_start_date"], end_date=for_date
    )
    cq_regional_bookings = bookings_by_region(
        data=data,
        start_date=date_values["date"]["cq_start_date"],
        end_date=for_date,
    )
    cq_comms_vs_identity_pipeline = pipeline_by_comms_vs_identity(
        data=data, start_date=date_values["date"]["cq_start_date"], end_date=for_date
    )
    cq_regional_pipeline = pipeline_by_region(
        data=data, start_date=date_values["date"]["cq_start_date"], end_date=for_date
    )
    cw_top_booked_opps = top_opps_in_period(
        data=data,
        category=Category.BOOKED,
        start_date=date_values["date"]["cw_start_date"],
        end_date=date_values["date"]["cw_end_date"],
    )
    cm_top_commits = top_commits_in_period(
        data=data,
        start_date=date_values["date"]["cm_start_date"],
        end_date=date_values["date"]["cm_end_date"],
    )
    cq_top_commits = top_commits_in_period(
        data=data,
        start_date=date_values["date"]["cq_start_date"],
        end_date=date_values["date"]["cq_end_date"],
        exclude=cm_top_commits,
    )
    cq_top_bc = top_best_case_in_period(
        data=data,
        start_date=date_values["date"]["cq_start_date"],
        end_date=date_values["date"]["cq_end_date"],
    )
    cq_top_bt = top_business_terms_in_period(
        data=data,
        start_date=date_values["date"]["cq_start_date"],
        end_date=date_values["date"]["cq_end_date"],
    )
    cw_top_created_opps = top_opps_in_period(
        data=data,
        category=Category.STAGE_1,
        start_date=date_values["date"]["cw_start_date"],
        end_date=date_values["date"]["cw_end_date"],
    )

    weekly_update_data = {
        "week_end_date_long": date_values["date"]["cw_end_date"].strftime("%B %d"),
        "cq_na_booked_percent": fmt_percentage(
            cq_regional_bookings["PERCENT"]["North America"], 0
        ),
        "cq_row_booked_percent": fmt_percentage(
            sum(cq_regional_bookings["PERCENT"].values())
            - cq_regional_bookings["PERCENT"]["North America"],
            places=0,
        ),
        "cq_comms_booked_percent": fmt_percentage(
            cq_comms_vs_id_bookings["PERCENT"]["Communications"]
        ),
        "cq_di_booked_percent": fmt_percentage(
            cq_comms_vs_id_bookings["PERCENT"]["Identity"]
        ),
        "cq_bundle_booked_percent": fmt_percentage(
            cq_comms_vs_id_bookings["PERCENT"]["Bundle"]
        ),
        "cq_services_booked_percent": fmt_percentage(
            cq_comms_vs_id_bookings["PERCENT"]["Services"]
        ),
        "ytd_bookings": fmt_currency(ytd_regional_bookings["DM"]["Total"], places=1),
        "ytd_na_bookings_percent": fmt_percentage(
            ytd_regional_bookings["PERCENT"]["North America"], places=0
        ),
        "ytd_row_bookings_percent": fmt_percentage(
            sum(ytd_regional_bookings["PERCENT"].values())
            - ytd_regional_bookings["PERCENT"]["North America"],
            places=0,
        ),
        "ytd_comms_bookings_percent": fmt_percentage(
            ytd_comms_vs_id_bookings["PERCENT"]["Communications"]
        ),
        "ytd_di_bookings_percent": fmt_percentage(
            ytd_comms_vs_id_bookings["PERCENT"]["Identity"]
        ),
        "ytd_bundle_bookings_percent": fmt_percentage(
            ytd_comms_vs_id_bookings["PERCENT"]["Bundle"]
        ),
        "ytd_tsp_bookings_percent": fmt_percentage(
            ytd_comms_vs_id_bookings["PERCENT"]["Services"]
        ),
        "mgmt_call": fmt_currency(management_call, 0),
        "cq_number": date_values["quarter"]["cq"].quarter(),
        "cq_commit_dm": fmt_currency(cq_forecast["DM"]["Commit"], 1),
        "cq_best_case_dm": fmt_currency(cq_forecast["DM"]["Best Case"], 1),
        "cq_pipeline_dm": fmt_currency(cq_forecast["DM"]["Pipeline"], 1),
        "cq_booked_dm": fmt_currency(cq_forecast["DM"]["Won"], 1),
        "dm_target": fmt_currency(quarterly_booking_target, 1),
        "cq_minus_4": date_values["quarter"]["cq_minus_4"],
        "cq_minus_4_booked_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.BOOKED,
                metric=Metric.DM,
                start_date=date_values["quarter"]["cq_minus_4"].start_date(),
                end_date=date_values["quarter"]["cq_minus_4"].end_date(),
            ),
            1,
        ),
        "cq_minus_3": date_values["quarter"]["cq_minus_3"],
        "cq_minus_3_booked_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.BOOKED,
                metric=Metric.DM,
                start_date=date_values["quarter"]["cq_minus_3"].start_date(),
                end_date=date_values["quarter"]["cq_minus_3"].end_date(),
            ),
            1,
        ),
        "cq_minus_2": date_values["quarter"]["cq_minus_2"],
        "cq_minus_2_booked_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.BOOKED,
                metric=Metric.DM,
                start_date=date_values["quarter"]["cq_minus_2"].start_date(),
                end_date=date_values["quarter"]["cq_minus_2"].end_date(),
            ),
            1,
        ),
        "cq_minus_1": date_values["quarter"]["cq_minus_1"],
        "cq_minus_1_booked_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.BOOKED,
                metric=Metric.DM,
                start_date=date_values["quarter"]["cq_minus_1"].start_date(),
                end_date=date_values["quarter"]["cq_minus_1"].end_date(),
            ),
            1,
        ),
        "cw_stage_1_opps": total_in_period(
            data=data,
            category=Category.STAGE_1,
            metric=Metric.COUNT,
            start_date=date_values["date"]["cw_start_date"],
            end_date=date_values["date"]["cw_end_date"],
        ),
        "cw_stage_1_opp_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.STAGE_1,
                metric=Metric.DM,
                start_date=date_values["date"]["cw_start_date"],
                end_date=date_values["date"]["cw_end_date"],
            ),
            1,
        ),
        "cq_stage_1_opps": total_in_period(
            data=data,
            category=Category.STAGE_1,
            metric=Metric.COUNT,
            start_date=date_values["date"]["cq_start_date"],
            end_date=date_values["date"]["cq_end_date"],
        ),
        "cq_stage_1_opp_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.STAGE_1,
                metric=Metric.DM,
                start_date=date_values["date"]["cq_start_date"],
                end_date=date_values["date"]["cq_end_date"],
            ),
            1,
        ),
        "cw_booked_opps": total_in_period(
            data=data,
            category=Category.BOOKED,
            metric=Metric.COUNT,
            start_date=date_values["date"]["cw_start_date"],
            end_date=date_values["date"]["cw_end_date"],
        ),
        "cw_booked_dm": fmt_currency(
            total_in_period(
                data=data,
                category=Category.BOOKED,
                metric=Metric.DM,
                start_date=date_values["date"]["cw_start_date"],
                end_date=date_values["date"]["cw_end_date"],
            ),
            1,
        ),
        "cw_booked_opp_1": f"{cw_top_booked_opps[0][0]} - {fmt_currency(cw_top_booked_opps[0][1],1)}",
        "cw_booked_opp_2": f"{cw_top_booked_opps[1][0]} - {fmt_currency(cw_top_booked_opps[1][1],1)}",
        "cw_booked_opp_3": f"{cw_top_booked_opps[2][0]} - {fmt_currency(cw_top_booked_opps[2][1],1)}",
        "current_month": date_values["date"]["cm_name"],
        "cm_commit_opp_1": f"{cm_top_commits[0][0]} - {fmt_currency(cm_top_commits[0][1],1)}",
        "cm_commit_opp_2": f"{cm_top_commits[1][0]} - {fmt_currency(cm_top_commits[1][1],1)}",
        "cm_commit_opp_3": f"{cm_top_commits[2][0]} - {fmt_currency(cm_top_commits[2][1],1)}",
        "cq_commit_opp_1": f"{cq_top_commits[0][0]} - {fmt_currency(cq_top_commits[0][1],1)}",
        "cq_commit_opp_2": f"{cq_top_commits[1][0]} - {fmt_currency(cq_top_commits[1][1],1)}",
        "cq_commit_opp_3": f"{cq_top_commits[2][0]} - {fmt_currency(cq_top_commits[2][1],1)}",
        "cq_best_case_opp_1": f"{cq_top_bc[0][0]} - {fmt_currency(cq_top_bc[0][1],1)}",
        "cq_best_case_opp_2": f"{cq_top_bc[1][0]} - {fmt_currency(cq_top_bc[1][1],1)}",
        "cq_best_case_opp_3": f"{cq_top_bc[2][0]} - {fmt_currency(cq_top_bc[2][1],1)}",
        "business_terms_opp_1": f"{cq_top_bt[0][0]} - {fmt_currency(cq_top_bt[0][1],1)}",
        "business_terms_opp_2": f"{cq_top_bt[1][0]} - {fmt_currency(cq_top_bt[1][1],1)}",
        "business_terms_opp_3": f"{cq_top_bt[2][0]} - {fmt_currency(cq_top_bt[2][1],1)}",
        "cq_na_booked_dm": fmt_currency(cq_regional_bookings["DM"]["North America"], 1),
        "cq_latam_booked_dm": fmt_currency(cq_regional_bookings["DM"]["LATAM"], 1),
        "cq_emea_booked_dm": fmt_currency(cq_regional_bookings["DM"]["EMEA"], 1),
        "cq_apac_booked_dm": fmt_currency(cq_regional_bookings["DM"]["APAC"], 1),
        "cw_created_opp_1": f"{cw_top_created_opps[0][0]} - {fmt_currency(cw_top_created_opps[0][1],1)}",
        "cw_created_opp_2": f"{cw_top_created_opps[1][0]} - {fmt_currency(cw_top_created_opps[1][1],1)}",
        "cw_created_opp_3": f"{cw_top_created_opps[2][0]} - {fmt_currency(cw_top_created_opps[2][1],1)}",
        "qtd_pipeline_attainment": qtd_pipeline_attainment,
        "cq_comms_pipeline_dm": fmt_currency(
            cq_comms_vs_identity_pipeline["DM"]["Communications"], 1
        ),
        "cq_di_pipeline_dm": fmt_currency(
            cq_comms_vs_identity_pipeline["DM"]["Identity"], 1
        ),
        "cq_bundle_pipeline_dm": fmt_currency(
            cq_comms_vs_identity_pipeline["DM"]["Bundle"], 1
        ),
        "cq_support_pipeline_dm": fmt_currency(
            cq_comms_vs_identity_pipeline["DM"]["Services"], 1
        ),
        "cq_comms_pipeline_percent": fmt_percentage(
            cq_comms_vs_identity_pipeline["PERCENT"]["Communications"]
        ),
        "cq_di_pipeline_percent": fmt_percentage(
            cq_comms_vs_identity_pipeline["PERCENT"]["Identity"]
        ),
        "cq_bundle_pipeline_percent": fmt_percentage(
            cq_comms_vs_identity_pipeline["PERCENT"]["Bundle"]
        ),
        "cq_support_pipeline_percent": fmt_percentage(
            cq_comms_vs_identity_pipeline["PERCENT"]["Services"]
        ),
        "cq_apac_pipeline_dm": fmt_currency(cq_regional_pipeline["DM"]["APAC"], 1),
        "cq_emea_pipeline_dm": fmt_currency(cq_regional_pipeline["DM"]["EMEA"], 1),
        "cq_latam_pipeline_dm": fmt_currency(cq_regional_pipeline["DM"]["LATAM"], 1),
        "cq_na_pipeline_dm": fmt_currency(
            cq_regional_pipeline["DM"]["North America"], 1
        ),
        "cq_apac_pipeline_percent": fmt_percentage(
            cq_regional_pipeline["PERCENT"]["APAC"]
        ),
        "cq_emea_pipeline_percent": fmt_percentage(
            cq_regional_pipeline["PERCENT"]["EMEA"]
        ),
        "cq_latam_pipeline_percent": fmt_percentage(
            cq_regional_pipeline["PERCENT"]["LATAM"]
        ),
        "cq_na_pipeline_percent": fmt_percentage(
            cq_regional_pipeline["PERCENT"]["North America"]
        ),
    }

    if management_call > 0:
        weekly_update_data["gap_dm"] = fmt_currency(
            cq_forecast["Management Call"]["DM Gap"], 1
        )
        weekly_update_data["gap_coverage"] = cq_forecast["Management Call"]["Coverage"]

    return weekly_update_data
