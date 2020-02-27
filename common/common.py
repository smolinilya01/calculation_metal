"""Common functions"""

import re
import logging

from typing import Union
from datetime import datetime
from pandas import (DataFrame, Series)


def modify_col(col: Series, instr=0, space=0, comma=0, numeric=0, minus=0) -> Series:
    """Изменяет колонку в зависимости от вида:

    :param col: колнка, которую нужно поменять
    :param instr: если 1, то в стринговое значение
    :param space: если 1, то удаляет пробелы
    :param comma: если 1, то заменяет запятые на точки в цифрах
    :param numeric: если 1, то в число с точкой
    :param minus: если 1, то минусовое число в ноль
    """
    if instr == 1:
        col = col.map(str)
    if space == 1:
        col = col.map(del_space)
    if comma == 1:
        col = col.map(replace_comma)
    if numeric == 1:
        col = col.map(float)
    if minus == 1:
        col = col.map(lambda x: 0 if x < 0 else x)
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


def extract_product_name(product_: str) -> str:
    """Извлекает наименование продукта заказа из стрингового значения

    :param product_: название продукта из ERP формата 'Номер заказа//Наименование продукта'
    """
    sep = '//'

    if sep in product_:
        name = product_.split(sep)[1]
        return name
    else:
        return product_
