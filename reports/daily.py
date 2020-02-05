"""Daily reports"""

from etl.extract import NOW
from datetime import timedelta
from pandas import (
    DataFrame, concat, read_csv
)


def daily_tables() -> None:
    """Создание таблиц для ежедневных отчетов"""
    name_output_req = r'.\support_data\output_tables\ask_{0}.csv'.format(NOW.strftime('%Y%m%d'))
    output_req = read_csv(
        name_output_req,
        sep=";",
        encoding='ansi',
        parse_dates=['Дата запуска']
    )
    deficit(output_req)


def deficit(table_: DataFrame) -> None:
    """Создание отчета по дефициту на сегодня + 4 дня вперед

    :param table_: главная таблица output_req
    """
    need_columns = [
        'Номер победы', 'Партия', 'Дата запуска',
        'Номенклатура', 'Количество в заказе',
        'Заказчик', 'Спецификация', 'Остаток дефицита'
    ]
    table: DataFrame = table_.copy()
    table['Остаток дефицита'] = table['Остаток дефицита'] + table['Списание из Поступлений']
    table['Заказчик'] = table['Заказчик'].replace({0: 'Омский ЭМЗ', '0': 'Омский ЭМЗ'})
    table = table[(table['Заказ обеспечен'] == 0) & (table['Пометка удаления'] == 0)]
    end_date = NOW + timedelta(days=4)
    table = table[table['Дата запуска'] <= end_date]
    table = table[need_columns]

    first_table = main_deficit_table(table)
    first_table.to_csv(
        r'.\support_data\data_for_reports\daily_deficit_1.csv',
        sep=";",
        encoding='ansi',
        index=False
    )

    second_table = second_deficit_table(first_table)
    second_table.to_csv(
        r'.\support_data\data_for_reports\daily_deficit_2.csv',
        sep=";",
        encoding='ansi',
        index=False
    )


def main_deficit_table(table: DataFrame) -> DataFrame:
    """Создание заготовки главной таблицы ежедневного отчета по дефициту

    :param table: таблица с подготовленными данными output_req из deficit()
    """
    group_columns = [
        'Номер победы', 'Партия', 'Дата запуска',
        'Заказчик', 'Спецификация'
    ]
    detail_table = table.groupby(by=group_columns).sum().reset_index()
    detail_table['Обеспеченность'] = 1 - (detail_table['Остаток дефицита'] / detail_table['Количество в заказе'])
    detail_table['Остаточная потребность'] = None
    detail_table['Дата запуска ФАКТ'] = None  # потом заменится на существующую колонку
    detail_table = detail_table[detail_table['Остаток дефицита'] > 0.5]
    detail_table = detail_table.sort_values(by=['Дата запуска'])

    first_table = list()
    for i in range(len(detail_table)):  # заполнение первой таблицы отчета
        row = detail_table.iloc[i]
        first_table.append(row.to_list())

        nomenclature_row = table[
            (table['Номер победы'] == row['Номер победы']) &
            (table['Партия'] == row['Партия']) &
            (table['Остаток дефицита'] > 0)
        ].copy()
        nomenclature_row['Заказчик'] = nomenclature_row['Номенклатура']
        nomenclature_row['Остаточная потребность'] = nomenclature_row['Остаток дефицита']
        nomenclature_row['Обеспеченность'] = None
        nomenclature_row['Дата запуска ФАКТ'] = None
        row_columns = set(table.columns) - {'Заказчик', 'Остаточная потребность'}
        nomenclature_row[list(row_columns)] = None
        nomenclature_row = nomenclature_row[detail_table.columns]
        for ii in range(len(nomenclature_row)):
            first_table.append(nomenclature_row.iloc[ii].to_list())

    # работа с колонками
    first_table = DataFrame(data=first_table, columns=detail_table.columns)
    first_table = first_table[[
        'Дата запуска', 'Дата запуска ФАКТ', 'Заказчик',
        'Спецификация', 'Номер победы', 'Партия',
        'Остаточная потребность', 'Обеспеченность'
    ]]
    first_table = first_table.rename(columns={
        'Дата запуска': 'Дата запуска ПЛАН',
        'Заказчик': 'Заказчик/Сортамент',
        'Номер победы': '№ заказа'
    })
    first_table['Дата закрытия дефицита'] = None
    first_table['Примечание МТО'] = None
    first_table['Примечание ПО'] = None
    return first_table


def second_deficit_table(table: DataFrame) -> DataFrame:
    """Создание второй таблицы ежедневного отчета по дефициту

    :param table: таблица из main_deficit_table
    """
    launch = table[(~table['Дата запуска ФАКТ'].isna()) & (table['Дата запуска ПЛАН'].isna())]
    non_launch = table[(table['Дата запуска ФАКТ'].isna()) & (table['Дата запуска ПЛАН'].isna())]

    need_columns = ['Заказчик/Сортамент', 'Остаточная потребность']
    launch, non_launch = launch[need_columns], non_launch[need_columns]
    launch = launch.groupby(by=['Заказчик/Сортамент']).sum().reset_index()
    non_launch = non_launch.groupby(by=['Заказчик/Сортамент']).sum().reset_index()

    second_table = DataFrame(data=None, columns=launch.columns)
    second_table.loc[0] = ['В работе', launch['Остаточная потребность'].sum()]
    second_table = concat([second_table, launch])

    second_table.loc[len(second_table)] = ['Не в работе', non_launch['Остаточная потребность'].sum()]
    second_table = concat([second_table, non_launch])

    summary_deficit = second_table['Остаточная потребность'][
        second_table['Заказчик/Сортамент'].isin(['В работе', 'Не в работе'])
    ].sum()
    second_table.loc[len(second_table)] = ['ИТОГО', summary_deficit]

    second_table.columns = ['Номенклатура металла', 'Потребность']
    second_table['Комментарий МТО'] = None
    return second_table
