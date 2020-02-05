"""Extract data"""

import logging

from common.common import (
    modify_col, replace_minus, extract_product_name
)
from pandas import (
    DataFrame, read_csv, read_excel, merge
)
from datetime import datetime

NOW: datetime = datetime.now()


def requirements() -> DataFrame:
    """Загузка таблицы с первичной потребностью (дефицитом), форматирование таблицы."""
    path = r'support_data/outloads/ask.txt'
    data = read_csv(
        path, 
        sep='\t',
        encoding='ansi', 
        parse_dates=['Дата запуска'],
        dayfirst=True
    )
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
    data = data.sort_values(by=['Дата запуска', 'Заказ-Партия'])  # сортировка потребности и определение
    data = data.reset_index().rename(columns={'index': 'Поряд_номер'})  # определение поряд номера

    logging.info('Потребность загрузилась')
    return data


def nomenclature() -> DataFrame:
    """Загузка таблицы с номенклатурой и ее полями для определения замен
    , форматирование таблицы.
    """
    path = r'support_data/outloads/dict_nom.xlsx'
    data = read_excel(path, sheet_name='1').drop_duplicates()
    data = data.rename(columns={'Номенклатура': 'index'}).set_index('index', drop=False)  # помещение названия в индекс
    # колонка названия номенклатуры остается и в таблице и в индексе для дальнейшей работы
    data = data.rename(columns={'index': 'Номенклатура'})
    data['Сортамет+Марка'] = data['Сортамент'] + '-' + data['Марка-категория']  # Создание столбца Сортам_маркак

    logging.info('Номенклатура загрузилась')
    return data


def replacements() -> DataFrame:
    """Загузка таблицы с заменами, форматирование таблицы."""
    path = r'support_data/outloads/dict_replacement.csv'
    data = read_csv(path, sep=';', encoding='ansi')

    logging.info('Замены загрузились')
    return data


def center_rests(nom_: DataFrame) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.

    :param nom_: таблица из nomenclature() - справочник номенклатуры
    """
    path = r'support_data/outloads/rest_center.txt'
    data = read_csv(path, sep='\t', encoding='ansi')
    data = data.merge(nom_[['Номенклатура', 'Сортамет+Марка']], on='Номенклатура', how='left')
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'Центральный склад'
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)
    data = data.fillna(0)
    data = data.sort_values(by='Дата')

    logging.info('Остатки центрального склада загрузились')
    return data


def tn_rests(nom_: DataFrame) -> DataFrame:
    """Загрузка таблицы с остатками на складе ТН, форматирование таблицы.

    :param nom_: таблица из nomenclature() - справочник номенклатуры
    """
    path = r'support_data/outloads/rest_tn.txt'
    data = read_csv(path, sep='\t', encoding='ansi')
    data = data.merge(nom_[['Номенклатура', 'Сортамет+Марка']], on='Номенклатура', how='left')
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'ТН'
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)
    data = data.fillna(0)
    data = data.sort_values(by='Дата')

    logging.info('Остатки склада ТН загрузились')
    return data


def future_inputs(nom_: DataFrame) -> DataFrame:
    """Загрузка таблицы с поступлениями, форматирование таблицы.

    :param nom_: таблица из nomenclature() - справочник номенклатуры
    """
    path = r'support_data/outloads/rest_futures_inputs.csv'
    data = read_csv(
        path,
        sep=';',
        encoding='ansi',
        parse_dates=['Дата'],
        dayfirst=True
    )
    data = data.merge(nom_[['Номенклатура', 'Сортамет+Марка']], on='Номенклатура', how='left')
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
