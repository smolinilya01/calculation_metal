"""Extract data"""

import logging
import re

from common.common import (
    modify_col, replace_minus, extract_product_name
)
from pandas import (
    DataFrame, read_csv, merge, NaT, read_sql_query
)
from datetime import datetime, timedelta
from numpy import nan
from pypyodbc import connect

NOW: datetime = datetime.now()
DAYS_AFTER: int = 4  # для расчета дневного дефицита, определеяет период от сегодня + 4 дня


def requirements(short_term_plan: bool = False) -> DataFrame:
    """Загузка таблицы с первичной потребностью (дефицитом), форматирование таблицы."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Расчет металл (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        parse_dates=['Дата запуска', 'Дата начала факт', 'Дата поступления КМД'],
        dayfirst=True
    )
    if short_term_plan is True:  # для краткосрочного планирования сразу обрезаем по дате
        end_date = NOW + timedelta(days=DAYS_AFTER)
        data = data[data['Дата запуска'] <= end_date]

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
    data['Перемещено'] = modify_col(data['Перемещено'], instr=1, space=1, comma=1, numeric=1, minus=1)
    data['Заказ обеспечен'] = modify_col(data['Заказ обеспечен'], instr=1, space=1, comma=1, numeric=1)
    data['Пометка удаления'] = modify_col(data['Пометка удаления'], instr=1, space=1, comma=1, numeric=1)
    data['Заказ-Партия'] = data['Номер победы'] + "-" + data['Партия']

    # добавляет колонки 'Закуп подтвержден', 'Возможный заказ' по данным из ПОБЕДЫ
    appr_orders = approved_orders(tuple(data['Номер победы'].unique()))
    data = merge(data, appr_orders, how='left', on='Номер победы', copy=False)
    data['Дефицит'] = data['Дефицит'].where(
        (data['Заказ обеспечен'] == 0) &
        (data['Пометка удаления'] == 0) &
        (data['Закуп подтвержден'] == 1),
        0
    )
    data['Изделие'] = modify_col(data['Изделие'], instr=1).map(extract_product_name)
    del data['Обеспечена метизы']  # del data['Обеспечена метизы'], data['Заказчик'], data['Спецификация']

    tn_ord = tn_orders()
    data = merge(data, tn_ord, how='left', on='Заказ-Партия', copy=False)  # индикатор ТН в таблицу потребности

    if short_term_plan is True:  # для краткосрочного планирования сразу обрезаем по дате
        data = multiple_sort(data)  # сортировка потребности и определение
        data = data[~data['Номенклатура'].str.contains(r'Табличка', regex=True)]  # убираем все таблички
    else:
        data = data.sort_values(by='Дата запуска')  # сортировка потребности и определение

    data = data.reset_index().rename(columns={'index': 'Поряд_номер'})  # определение поряд номера

    logging.info('Потребность загрузилась')
    return data


def nomenclature() -> DataFrame:
    """Загузка таблицы со структурными данными для замен."""
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\Справочник_металла (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    )
    """помещение названия в индекс
    колонка названия номенклатуры остается и в таблице и в индексе для дальнейшей работы"""
    data = data.\
        rename(columns={'Номенклатура': 'index'}).\
        set_index('index', drop=False).\
        rename(columns={'index': 'Номенклатура'}). \
        fillna('')

    data['Марка-категория'] = (
        data['Номенклатура.Марка стали'] + '-' +
        data['Номенклатура.Категория стали']
    )
    data['Сортамент'] = data.iloc[:, :3].apply(lambda x: create_sortam(x), axis=1)
    data = data.rename(columns={
        'Номенклатура.Стандарт на сортамент (ГОСТ)': 'ГОСТ Сортамента',
        'Номенклатура.Марка стали': 'Марка',
        'Номенклатура.Категория стали': 'Категория'
    })
    data = data[[
        'Номенклатура', 'Сортамент',
        'ГОСТ Сортамента', 'Марка',
        'Категория', 'Марка-категория'
    ]]
    data['Сортамет+Марка'] = (
            data['Сортамент'].map(str.strip) + '-' +
            data['Марка-категория'].map(str.strip)
    )  # Создание столбца Сортам_маркак

    logging.info('Номенклатура загрузилась')
    return data


def create_sortam(x) -> str:
    """Создание сортамента из наименования, вида и госта"""
    nom, vid, gost = str(x[0]).strip(), str(x[1]).strip(), str(x[2]).strip()
    if '' in [nom, vid, gost]:
        return ''

    sortam = re.search(f'{vid}(\s*.+)\s*{gost}', nom).group(1)

    return vid + sortam.rstrip()


def replacements() -> DataFrame:
    """Загузка таблицы с заменами марки и категории."""
    path = r'.\support_data\outloads\dict_replacement.csv'
    data = read_csv(
        path,
        sep=';',
        encoding='ansi'
    )

    logging.info('Замены загрузились')
    return data


def center_rests(dictionary: DataFrame, short_term_plan=False) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.
    Колонка с количеством остатком должна иметь наименование "Количество".

    :param dictionary: таблица из nomenclature() - справочник номенклатуры
    :param short_term_plan: если True, то запись остаток в папку для сохранения прошлых расчетов
    """
    path = r"\\oemz-fs01.oemz.ru\Works$\Analytics\Илья\!outloads\!metal_center+metiz (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi'
    ).rename(columns={'Конечный остаток': 'Количество'})
    data = data.rename(columns={'Артикул': 'Код'})
    data = data[~data['Номенклатура'].isna()]
    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data = data[data['Количество'] > 0]
    data['Склад'] = 'Центральный склад'  # Склады центральные по металлу, метизам и вход контроля
    data['Дата'] = datetime(NOW.year, NOW.month, NOW.day)
    data = data.merge(dictionary, on='Номенклатура', how='left')

    if short_term_plan is True:
        data.to_csv(
            f'W:\\Analytics\\Илья\\!deficit_work_files\\rests_center_mtz {NOW.strftime("%y%m%d %H_%M_%S")}.csv',
            sep=";",
            encoding='ansi',
            index=False
        )  # запись используемых файлов, для взгляда в прошлое

    logging.info('Остатки центрального склада загрузились')
    return data


def tn_rests(dictionary: DataFrame, short_term_plan=False) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.
    Колонка с количеством остатком должна иметь наименование "Количество".

    :param dictionary: таблица из nomenclature() - справочник номенклатуры
    :param short_term_plan: если True, то запись остаток в папку для сохранения прошлых расчетов
    """
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
    data = data.merge(dictionary, on='Номенклатура', how='left')

    if short_term_plan is True:
        data.to_csv(
            f'W:\\Analytics\\Илья\\!deficit_work_files\\rests_tn {NOW.strftime("%y%m%d %H_%M_%S")}.csv',
            sep=";",
            encoding='ansi',
            index=False
        )  # запись используемых файлов, для взгляда в прошлое

    logging.info('Остатки склада ТН загрузились')
    return data


def future_inputs(dictionary: DataFrame, short_term_plan=False) -> DataFrame:
    """Загузка таблицы с остатками на центральном складе, форматирование таблицы.
    Колонка с количеством остатком должна иметь наименование "Количество".

    :param dictionary: таблица из nomenclature() - справочник номенклатуры
    :param short_term_plan: если True, то запись остаток в папку для сохранения прошлых расчетов
    """
    path = r"W:\Analytics\Илья\!outloads\Остатки заказов поставщикам металл (ANSITXT).txt"
    data = read_csv(
        path,
        sep='\t',
        encoding='ansi',
        usecols=['Дата поступления', 'Номенклатура', 'Заказано остаток']
    ).rename(
        columns={'Дата поступления': 'Дата', 'Заказано остаток': 'Количество'}
    ).dropna()
    data['Дата'] = data['Дата'].map(lambda x: datetime.strptime(x, '%d.%m.%Y'))

    data['Количество'] = modify_col(data['Количество'], instr=1, space=1, comma=1, numeric=1)
    data['Склад'] = 'Поступления'
    data = data.\
        fillna(0).\
        sort_values(by='Дата')
    data = data.merge(dictionary, on='Номенклатура', how='left')

    if short_term_plan is True:
        data = DataFrame(data=None, columns=list(data.columns))  # дневной дефицит без поступлений

    data.to_csv(
        r".\support_data\outloads\rest_futures_inputs.csv",
        sep=";",
        encoding='ansi',
        index=False
    )  # запись используемых файлов, для взгляда в прошлое

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


def long_term_sortaments() -> DataFrame:
    """Загрузка таблицы с сортаментами длинной поставки."""
    path = r'support_data/outloads/long_term_sortaments.csv'
    data = read_csv(
        path,
        sep=';',
        encoding='ansi'
    )

    logging.info('Длинные сортаменты загрузились')
    return data


def approved_orders(orders: tuple) -> DataFrame:
    """Заказы с подтверждением закупа материалов по ним.

    :param orders: список уникальных заказов
    """
    with open(r'support_data/outloads/query_approved_orders.sql') as file:
        query = file.read().format(orders)
    connection = connect(
        "Driver={SQL Server};"
        "Server=OEMZ-POBEDA;"
        "Database=ProdMgrDB;"
        "uid=1C_Exchange;pwd=1"
    )
    data = read_sql_query(query, connection)
    data['Закуп подтвержден'] = data['level_of_allowing'].map(lambda x: 1 if x >= 5 else 0)
    data['Закуп подтвержден'] = data['Закуп подтвержден'].where(
        data['number_order'].map(lambda x: False if x[1] == '0' else True),
        1
    )
    data['Возможный заказ'] = data['level_of_allowing'].map(lambda x: 1 if x == 4 else 0)
    data = data.rename(columns={'number_order': 'Номер победы'})
    return data[['Номер победы', 'Закуп подтвержден', 'Возможный заказ']]
