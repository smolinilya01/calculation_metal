"""Run script"""

import logging

from algo.write_off import write_off
from etl.extract import (
    nomenclature, replacements, center_rests,
    tn_rests, future_inputs, requirements
)
from common.common import (
    check_missing_nomenclature, check_calculation_right
)


def main() -> None:
    """Выполнение расчетов"""
    dict_nom = nomenclature()
    dict_repl = replacements()
    operations = list()

    start_rest_center = center_rests(dict_nom)
    start_rest_tn = tn_rests(dict_nom)
    start_fut = future_inputs(dict_nom)
    start_ask = requirements()

    end_rest_center = start_rest_center.copy()
    end_rest_tn = start_rest_tn.copy()
    end_fut = start_fut.copy()
    end_ask = start_ask.copy()

    check_missing_nomenclature(
        rest_center_=start_rest_center,
        future_inputs_=start_fut,
        ask=start_ask,
        rest_tn=start_rest_tn,
        nom_=dict_nom,
    )

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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # logging.disable(level=logging.CRITICAL)
    main()
