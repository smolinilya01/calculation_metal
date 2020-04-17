"""Create daily report
Для запуска ежедневного отчета по дефициту"""

import logging

from os import chdir
from algo.write_off import write_off
from etl.extract import (
    replacements, center_rests,
    tn_rests, future_inputs, requirements,
    nomenclature
)
from common.common import check_calculation_right
from traceback import format_exc
from reports.weekly import weekly_tables
from reports.daily import daily_tables
from reports.excel import daily_excel_reports


def main() -> None:
    """Выполнение расчетов"""
    operations = list()
    dict_nom = nomenclature()
    dict_repl = replacements()
    start_rest_center = center_rests(dictionary=dict_nom, short_term_plan=True)
    start_rest_tn = tn_rests(dictionary=dict_nom, short_term_plan=True)
    start_fut = future_inputs(dictionary=dict_nom, short_term_plan=True)
    start_ask = requirements(short_term_plan=True)

    end_rest_center = start_rest_center.copy()
    end_rest_tn = start_rest_tn.copy()
    end_fut = start_fut.copy()
    end_ask = start_ask.copy()

    # списание остатков на потребности
    end_ask, end_rest_tn, end_rest_center, end_fut, operations = write_off(
        table=end_ask,
        rest_tn=end_rest_tn,
        rest_c=end_rest_center,
        fut=end_fut,
        oper_=operations,
        nom_=dict_nom,
        repl_=dict_repl
    )

    check_calculation_right(
        start_ask_=start_ask,
        end_ask_=end_ask,
        start_c_=start_rest_center,
        end_c_=end_rest_center,
        start_tn_=start_rest_tn,
        end_tn_=end_rest_tn,
        start_fut_=start_fut,
        end_fut_=end_fut,
    )

    weekly_tables(
        start_ask_=start_ask,
        end_ask_=end_ask,
        oper_=operations,
        sep_date=None
    )
    daily_tables()
    daily_excel_reports()


if __name__ == '__main__':
    # chdir(r'C:\LOG_1\calculation_metal')  # что бы планировщик заданий переключался на правильную директорию
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=r'.\logs.txt'
    )
    # logging.disable(level=logging.CRITICAL)
    try:
        main()
    except Exception:
        logging.info(format_exc())
