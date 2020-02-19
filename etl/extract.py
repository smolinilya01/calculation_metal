"""Extract data"""

import logging

from common.common import (
    modify_col, replace_minus, extract_product_name
)
from pandas import (
    DataFrame, read_csv, merge, NaT
)
from datetime import datetime
from numpy import nan

NOW: datetime = datetime.now()


def requirements() -> DataFrame:
    """Загузка таблицы с первичной потребностью (дефицитом), форматирование таблицы."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Расчет металл (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        parse_dates=['Дата запуска', 'Дата начала факт', 'Дата поступления КМД'],
        dayfirst=True
    )
    data = data[~data['Номенклатура'].isna()]
    data = data.fillna(value=0)
    data = data.rename(columns={'Обеспечена МП': 'Заказ обеспечен'})
    data['Заказ обеспечен'] = data['Заказ обеспечен'].replace({'Нет': 0, 'Да': 1})
    data['Пометка удаления'] = data['Пометка удаления'].replace({'Нет': 0, 'Да': 1})
    data['Номер победы'] = modify_col(data['Номер победы'], instr=1, space=1)
    data['Партия'] = data['Партия'].map(int)
    data['Партия'] = modify_col(data['Партия'], instr=1, space=1).replace({'0': '1', '0.0': '1'})
    data['Количество в заказе'] = modify_col(data['Количество в заказе'], instr=1, space=1, comma=1, numeric=1)
    data['Дефицит'] = modify_col(data['Дефицит'], instr=1, space=1, comma=1, numeric=1).map(replace_minus)
    data['Перемещено'] = modify_col(data['Перемещено'], instr=1, space=1, comma=1, numeric=1)
    data['Заказ обеспечен'] = modify_col(data['Заказ обеспечен'], instr=1, space=1, comma=1, numeric=1)
    data['Пометка удаления'] = modify_col(data['Пометка удаления'], instr=1, space=1, comma=1, numeric=1)
    data['Заказ-Партия'] = data['Номер победы'] + "-" + data['Партия']
    data['Дефицит'] = data['Дефицит'].where(
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0),
        0
    )
    data['Изделие'] = modify_col(data['Изделие'], instr=1).map(extract_product_name)
    del data['Обеспечена метизы']  # del data['Обеспечена метизы'], data['Заказчик'], data['Спецификация']

    tn_ord = tn_orders()
    data = merge(data, tn_ord, how='left', on='Заказ-Партия', copy=False)  # индикатор ТН в таблицу потребности
    data = multiple_sort(data)  # сортировка потребности и определение
    data = data.reset_index().rename(columns={'index': 'Поряд_номер'})  # определение поряд номера

    logging.info('Потребность загрузилась')
    return data


def replacements() -> DataFrame:
    """Загузка таблицы с заменами, форматирование таблицы."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Замены (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        parse_dates=['Период завершения'],
        dayfirst=True
    )
    renames = {
        'Материал.Код': 'Номенклатура.Код',
        'Материал': 'Номенклатура',
        'Коэффициент замены': 'Коэф_замены',
        'Период завершения': 'Завершение',
        'Аналог.Категория стали': 'Аналог.Кат_стали'
    }
    data = data.rename(columns=renames)
    data = data.sort_values(by=['Номенклатура', 'Аналог.Кат_стали'])

    logging.info('Замены загрузились')
    return data


def center_rests() -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\!metal_center+metiz (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    )
    data = data.rename(columns={'Конечный остаток': "Количество", 'Артикул': 'Код'})
    data = data[~data['Номенклатура'].isna()]
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data = data[data['Количество'] > 0]
    data['Склад'] = 'Центральный склад'  # Склады центральные по металлу, метизам и вход контроля
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)

    logging.info('Остатки центрального склада загрузились')
    return data


def tn_rests() -> DataFrame:
    """Загрузка таблицы с остатками на складе ТН, форматирование таблицы."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\!metal_tn (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
    )
    data = data.rename(columns={'Конечный остаток': "Количество", 'Артикул': 'Код'})
    data = data[~data['Номенклатура'].isna()]
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'ТН'
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)

    logging.info('Остатки склада ТН загрузились')
    return data


def future_inputs() -> DataFrame:
    """Загрузка таблицы с поступлениями, форматирование таблицы."""
    path = r'support_data/outloads/rest_futures_inputs.csv'
    data = read_csv(
        path,
        sep=';',
        encoding='ansi',
        parse_dates=['Дата'],
        dayfirst=True
    )
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'Поступления'
    data = data.fillna(0)
    data = data.sort_values(by='Дата')

    logging.info('Поступления загрузились')
    return data


def tn_orders() -> DataFrame:
    """Загрузка списка заказов по ТН"""
    path = r'support_data/outloads/dict_orders_tn.txt'
    data = read_csv(path, sep='\t', encoding='ansi').drop_duplicates()

    logging.info('Заказы ТН загрузилась')
    return data


def multiple_sort(table: DataFrame) -> DataFrame:
    """Многоступенчетая сортировка:
    Ранг -> Дата запуска факт -> Дата поступления КМД -> Дата запуска

    :param table: таблица из ask
    """
    data = table
    if 0 in data['Дата начала факт'][data['Дата начала факт'] == 0].values:
        data['Дата начала факт'] = data['Дата начала факт'].replace({0: NaT, 0.0: NaT})
    if 0 in data['Дата поступления КМД'][data['Дата поступления КМД'] == 0].values:
        data['Дата поступления КМД'] = data['Дата поступления КМД'].replace({0: NaT, 0.0: NaT})
    data['Приоритет'] = data['Приоритет'].replace({0: nan, 0.0: nan})
    sort_columns = [
        'Приоритет', 'Дата начала факт',
        'Дата поступления КМД', 'Дата запуска',
        'Заказ-Партия'
    ]
    data = data.sort_values(by=sort_columns)

    return data
