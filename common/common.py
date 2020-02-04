"""Common functions"""

import re
import sys
import logging

from typing import Union
from datetime import datetime
from pandas import (DataFrame, Series, concat)


def modify_col(col: Series, instr=0, space=0, comma=0, numeric=0) -> Series:
    """Изменяет колонку в зависимости от вида:

    :param col: колнка, которую нужно поменять
    :param instr: если 1, то в стринговое значение
    :param space: если 1, то удаляет пробелы
    :param comma: если 1, то заменяет запятые на точки в цифрах
    :param numeric: если 1, то в число с точкой
    """
    if instr == 1:
        col = col.map(str)
    if space == 1:
        col = col.map(del_space)
    if comma == 1:
        col = col.map(replace_comma)
    if numeric == 1:
        col = col.map(float)
    return col


def del_space(x: str) -> str:
    """Удаление пробелов."""
    return re.sub(r'\s', '', x)


def extract_day(x: datetime) -> int:
    """Дату превращает в день"""
    return x.days


def replace_comma(x: str) -> str:
    """Меняет запятую на точку"""
    return x.replace(',', '.')


def replace_minus(x: Union[float, int]) -> Union[float, int]:
    """Меняет отрицательное число на 0"""
    if x < 0:
        return 0
    else:
        return x


def check_missing_nomenclature(
    rest_center_: DataFrame,
    future_inputs_: DataFrame,
    ask: DataFrame,
    rest_tn: DataFrame,
    nom_: DataFrame,
) -> None:
    """Проверка наличия всех номенклатур в справочнике сортаментов и марок

    :param rest_center_: таблица остатков центрального склада
    :param future_inputs_: таблица поступлений
    :param ask: таблица изначальных потребностей
    :param rest_tn: таблица остатков склада ТН
    :param nom_: таблица справочника номенклатуры
    """
    all_nom = concat([
        rest_center_['Номенклатура'],
        future_inputs_['Номенклатура'],
        rest_tn['Номенклатура'],
        ask['Номенклатура']
    ]).drop_duplicates()
    out_nom = all_nom[~all_nom.isin(nom_['Номенклатура'])]

    if len(out_nom) > 0:
        print('Необходимо добавить следующие номенклатуры в справочник:')

        for i in list(out_nom):
            print(i)
        # input()
        sys.exit()  # если есть номенклатуры, которые нужно добавить в справочник


def check_calculation_right(
    start_ask_: DataFrame,
    end_ask_: DataFrame,
    start_c_: DataFrame,
    end_c_: DataFrame,
    start_tn_: DataFrame,
    end_tn_: DataFrame,
    start_fut_: DataFrame,
    end_fut_: DataFrame,
) -> None:
    """Проверка правильности расчета.

    :param start_ask_: таблица потребностей на начало расчета
    :param end_ask_: таблица потребностей на конец расчета (облегченная)
    :param start_c_: таблица остатков центрального склада на начало расчета
    :param end_c_: таблица остатков центрального склада на конец расчета
    :param start_tn_: таблица остатков склада ТН на начало расчета
    :param end_tn_: таблица остатков склада ТН на конец расчета
    :param start_fut_: таблица остатков поступлений на начало расчета
    :param end_fut_: таблица остатков поступлений на конец расчета
    """
    otklon_ask = start_ask_['Дефицит'].sum() - end_ask_['Дефицит'].sum()
    otklon_rest = (
            start_c_['Количество'].sum() - end_c_['Количество'].sum() +
            start_tn_['Количество'].sum() - end_tn_['Количество'].sum() +
            start_fut_['Количество'].sum() - end_fut_['Количество'].sum()
    )
    logging.info(
        'Таблицы подготовились: ' + str(round(otklon_ask, 3) == round(otklon_rest, 3))
    )
    logging.info(
        'Отклонение аска: ' + str(otklon_ask) +
        '; Отклонение остатков: ' + str(otklon_rest)
    )
    logging.info(
        'Потребность / ' + str(int(start_ask_['Дефицит'].sum())) +
        ' / ' + str(int(end_ask_['Дефицит'].sum()))
    )
