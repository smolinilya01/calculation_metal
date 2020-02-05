"""Building reports"""

from pandas import DataFrame
from reports.weekly import weekly_tables


def all_reports(
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
