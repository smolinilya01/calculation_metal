"""Create weekly report"""

import logging

from algo.write_off import write_off
from etl.extract import (
    replacements, center_rests,
    tn_rests, future_inputs, requirements,
    nomenclature
)
from common.common import check_calculation_right
from datetime import datetime
from reports.main import weekly_reports


def main() -> None:
    """Выполнение расчетов"""
    separate_date = input('Введите дату окончания краскосрочного периода,\nВ формате ДД.ММ.ГГГГ:\n')
    separate_date = datetime.strptime(separate_date, '%d.%m.%Y')

    operations = list()
    dict_nom = nomenclature()
    dict_repl = replacements()
    start_rest_center = center_rests(nom_=dict_nom)
    start_rest_tn = tn_rests(nom_=dict_nom)
    start_fut = future_inputs(nom_=dict_nom)
    start_ask = requirements()

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

    weekly_reports(
        start_ask_=start_ask,
        end_ask_=end_ask,
        oper_=operations,
        sep_date=separate_date
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # logging.disable(level=logging.CRITICAL)
    main()
