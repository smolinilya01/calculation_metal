"""Analysis of shipments
Анализ закупок менеджеров по отношению к недельному отчету расчету закупа"""

from etl.extract import (
    replacements, load_processed_deficit,
    load_orders_to_supplier, nomenclature
)
from algo.search import building_purchase_analysis


def main() -> None:
    """Главная функция анализа закупок"""
    processed_deficit = load_processed_deficit()
    orders = load_orders_to_supplier()

    dict_nom = nomenclature()
    dict_repl = replacements()

    building_purchase_analysis(
        table=processed_deficit,
        orders=orders,
        nom_=dict_nom,
        repl_=dict_repl
    )


if __name__ == '__main__':
    main()
