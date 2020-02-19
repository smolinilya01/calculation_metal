"""Building reports"""

from pandas import DataFrame
from reports.weekly import weekly_tables
from reports.daily import daily_tables
from reports.excel import (weekly_excel_reports, daily_excel_reports)


def weekly_reports(
    start_ask_: DataFrame,
    end_ask_: DataFrame,
    oper_: list
) -> None:
    """Построение всех отчетов

    :param start_ask_:
    :param end_ask_:
    :param oper_:
    """
    weekly_tables(
        start_ask_=start_ask_,
        end_ask_=end_ask_,
        oper_=oper_
    )
    weekly_excel_reports()


def daily_reports(
    start_ask_: DataFrame,
    end_ask_: DataFrame,
    oper_: list
) -> None:
    """Построение всех отчетов

    :param start_ask_:
    :param end_ask_:
    :param oper_:
    """
    weekly_tables(
        start_ask_=start_ask_,
        end_ask_=end_ask_,
        oper_=oper_
    )
    daily_tables()
    daily_excel_reports()
