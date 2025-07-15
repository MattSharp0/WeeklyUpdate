#! /Users/msharp/.pyenv/versions/weekly-update/bin/python

import typer
import datetime as dt
import logging
import pandas as pd

from decimal import Decimal
from dotenv import load_dotenv
from os import path, environ
from time import time
from string import Template

from data.query import run_salesforce_query, SALESFORCE_QUERY
from data.transformations import salesforce_dict_to_dataframe
from data.weekly_update import generate_date_inputs, generate_weekly_update_dict

from document_handler.terminal_handler import print_to_terminal
from document_handler.docx_handler import write_to_docx


def main(
    date_override: str = dt.date.today().isoformat(),
    skip_management_call: bool = False,
    verbose: bool = False,
    save_to_docx: bool = True,
    debug: bool = False
):
    """
    Generates weekly sales update text or .docx file for the current week.\n
    """

    logging.basicConfig(
        level=logging.INFO,
        # datefmt="%G-%m-%d %H:%M:%S",
        datefmt="%H:%M:%S",
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    
    log = logging.getLogger(__name__)

    if debug:
        log.setLevel(logging.DEBUG)

    log.info("Started")
    start = time()

    _CURRENT_DIRECTORY = path.dirname(path.realpath(__file__))
    log.debug("Running from '{}'".format(_CURRENT_DIRECTORY))

    load_dotenv(path.join(_CURRENT_DIRECTORY, ".env"), encoding="utf-8", override=True)
    _ca_bundle_path = environ[
        "REQUESTS_CA_BUNDLE"
    ]  # explicitly load CA Bundle Path from env

    try:
        input_date = dt.date.fromisoformat(date_override)
        log.debug("Date override supplied: {}".format(input_date))
    except ValueError as e:
        log.critical("date_override is not valid ISO format: 'YYYY-MM-DD'")
        raise ValueError(
            "{} is not valid ISO Format: 'YYYY-MM-DD'\n{}".format(date_override, e)
        )

    date_fmt_long = "%A, %B %-d"
    date_fmt_short = "%d%b%y"

    date_values = generate_date_inputs(date=input_date)

    if verbose:
        print(
            "\n\nGenerating weekly report for week of {} - {}".format(
                date_values["date"]["cw_start_date"].strftime(date_fmt_long),
                date_values["date"]["cw_end_date"].strftime(date_fmt_long),
            )
        )

    management_call = Decimal(0)

    if skip_management_call:
        template_name = "default_no_mgmt_call.txt"
    else:
        management_call = typer.prompt("\n\nPlease enter Management Call", type=Decimal)
        template_name = "default.txt"

    quarterly_booking_target = typer.prompt(
        "\n\nPlease enter Quarterly DM Target",
        type=Decimal,
        default=Decimal(1_100_000),
        show_default=True,
    )

    month_pipeline_target = typer.prompt(
        "\n\nPlease enter current month pipe gen target",
        type=Decimal,
        default=Decimal(861326),
        show_default=True,
    )

    min_date = date_values["quarter"]["cq_minus_4"].start_date().isoformat()

    salesforce_query = SALESFORCE_QUERY.substitute(
        {"MIN_DATE": min_date, "MIN_DATETIME": f"{min_date}T00:00:00.000Z"}
    )
    raw_data = run_salesforce_query(query=salesforce_query)
    data: pd.DataFrame = salesforce_dict_to_dataframe(raw_data=raw_data)

    template_data = generate_weekly_update_dict(
        data=data,
        management_call=management_call,
        monthly_pipe_target=month_pipeline_target,
        quarterly_booking_target=quarterly_booking_target,
    )

    template_path = path.join(_CURRENT_DIRECTORY, "templates", template_name)
    with open(template_path, mode="rt") as template_file:
        log.debug("Loaded template from {}".format(template_path))
        template = Template(template_file.read())

        weekly_update = template.safe_substitute(template_data)

    if verbose:
        print_to_terminal(
            weekly_update, week_start_date=input_date.strftime(date_fmt_long)
        )

    if save_to_docx:
        write_to_docx(
            weekly_update,
            week_start_date=dt.date.today().strftime(date_fmt_short),
            current_directory=_CURRENT_DIRECTORY,
        )
    log.info("Completed in {}ms".format(round(time()-start,2)))

if __name__ == "__main__":
    typer.run(main)
