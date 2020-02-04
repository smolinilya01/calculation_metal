"""Write off"""

from pandas import (DataFrame)


def write_off(
    table: DataFrame,
    rest_tn: DataFrame,
    rest_c: DataFrame,
    fut: DataFrame,
    oper_: list,
    nom_: DataFrame,
    repl_: DataFrame
) -> (DataFrame, DataFrame, DataFrame, DataFrame, list):
    """Процесс списания остатков и создания файлов csv"""
    index_clean_start_ask = table[
        (table['Дефицит'] > 0) &
        (table['Заказ обеспечен'] == 0) &
        (table['Пометка удаления'] == 0)
    ].index  # индексы по которым нужно пройтись

    for i in index_clean_start_ask:
        if table.at[i, 'ТН'] == 1:  # если заказ от ТН
            original(i, rest_tn, table, oper_)
            replacement(i, rest_tn, table, oper_, nom_, repl_)
            original(i, rest_c, table, oper_)
            replacement(i, rest_c, table, oper_, nom_, repl_)
            original(i, fut, table, oper_)
            replacement(i, fut, table, oper_, nom_, repl_)
        else:
            original(i, rest_c, table, oper_)
            replacement(i, rest_c, table, oper_, nom_, repl_)
            original(i, fut, table, oper_)
            replacement(i, fut, table, oper_, nom_, repl_)

    return table, rest_tn, rest_c, fut, oper_


def original(
    ind: int,
    sklad: DataFrame,
    table: DataFrame,
    oper_: list
) -> None:
    """Списание необходимой номенклатуры со склада

    :param ind: индекс строчки в таблице потребностей
    :param sklad: склад списания
    :param table: таблица потребностей
    :param oper_: писок операций
    """
    if table.at[ind, 'Дефицит'] == 0:
        pass
    else:
        cur_nom = table.at[ind, 'Номенклатура']
        index_rests = sklad[sklad['Номенклатура'] == cur_nom].sort_values(by='Дата').index

        for date in index_rests:  # date - это индекс найденных строчек в остатках по датам
            row_ask = table.loc[ind].copy()
            ask_start = row_ask['Дефицит']
            row_rest = sklad.loc[date].copy()
            rest_nom_start = row_rest['Количество']

            if rest_nom_start == 0:
                pass
            else:
                if (rest_nom_start - ask_start) < 0:
                    row_rest['Количество'] = 0
                    sklad.loc[date] = row_rest
                    row_ask['Дефицит'] = ask_start - rest_nom_start
                    table.loc[ind] = row_ask
                    row_for_oper = (
                        row_ask['Дата запуска'],
                        row_ask['Поряд_номер'],
                        row_ask['Заказ-Партия'],
                        cur_nom,
                        row_ask['Дефицит'],
                        ask_start,
                        ask_start - rest_nom_start,
                        rest_nom_start,
                        row_rest['Склад'],
                        row_rest['Дата'],
                        row_rest['Номенклатура'],
                        rest_nom_start,
                        0,
                        rest_nom_start
                    )
                    oper_.append(row_for_oper)
                else:
                    row_rest['Количество'] = rest_nom_start - ask_start
                    sklad.loc[date] = row_rest
                    row_ask['Дефицит'] = 0
                    table.loc[ind] = row_ask
                    row_for_oper = (
                        row_ask['Дата запуска'],
                        row_ask['Поряд_номер'],
                        row_ask['Заказ-Партия'],
                        cur_nom,
                        row_ask['Дефицит'],
                        ask_start,
                        0,
                        ask_start,
                        row_rest['Склад'],
                        row_rest['Дата'],
                        row_rest['Номенклатура'],
                        rest_nom_start,
                        rest_nom_start - ask_start,
                        ask_start
                    )
                    oper_.append(row_for_oper)
            if row_ask['Дефицит'] == 0:
                break  # если потребность 0, то следующая строчка


def replacement(
    ind: int,
    sklad: DataFrame,
    table: DataFrame,
    oper_: list,
    nom_: DataFrame,
    repl_: DataFrame
) -> None:
    """Списание со склада взаимозаменяемой номенклатуры

    :param ind: индекс строчки в таблице потребностей
    :param sklad: склад списания
    :param table: таблица потребностей
    :param oper_: писок операций
    :param nom_: справочник номенклатуры
    :param repl_: справочник замен
    """
    cur_nom = table.at[ind, 'Номенклатура']
    cur_markacat = nom_.at[cur_nom, 'Марка-категория']
    cur_sortam = nom_.at[cur_nom, 'Сортамент']

    if cur_markacat in repl_.columns:
        list_vsaim = cur_sortam + '-' + repl_[cur_markacat]
        list_vsaim = list_vsaim[list_vsaim.notna()]

        for zamen in list_vsaim:
            if table.at[ind, 'Дефицит'] == 0:
                break
            else:

                for i in sklad[sklad['Сортамет+Марка'] == zamen].sort_values(by='Дата').index:  # i - это индекс найденных строчек в остатках по датам
                    row_ask = table.loc[ind].copy()
                    ask_start = row_ask['Дефицит']
                    row_rest = sklad.loc[i].copy()
                    rest_nom_start = row_rest['Количество']

                    if rest_nom_start == 0:
                        pass
                    else:
                        if 0 > (rest_nom_start - ask_start):
                            row_rest['Количество'] = 0
                            sklad.loc[i] = row_rest
                            row_ask['Дефицит'] = ask_start - rest_nom_start
                            table.loc[ind] = row_ask

                            row_for_oper = (
                                table['Дата запуска'],
                                table['Поряд_номер'],
                                table['Заказ-Партия'],
                                cur_nom,
                                table['Дефицит'],
                                ask_start,
                                ask_start - rest_nom_start,
                                rest_nom_start,
                                row_rest['Склад'],
                                row_rest['Дата'],
                                row_rest['Номенклатура'],
                                rest_nom_start,
                                0,
                                rest_nom_start
                            )
                            oper_.append(row_for_oper)
                        else:
                            row_rest['Количество'] = rest_nom_start - ask_start
                            sklad.loc[i] = row_rest
                            row_ask['Дефицит'] = 0
                            table.loc[ind] = row_ask
                            row_for_oper = (
                                table['Дата запуска'],
                                table['Поряд_номер'],
                                table['Заказ-Партия'],
                                cur_nom,
                                table['Дефицит'],
                                ask_start,
                                0,
                                ask_start,
                                row_rest['Склад'],
                                row_rest['Дата'],
                                row_rest['Номенклатура'],
                                rest_nom_start,
                                rest_nom_start - ask_start,
                                ask_start
                            )
                            oper_.append(row_for_oper)
                    if row_ask['Дефицит'] == 0:
                        break  # если потребность 0, то следующая строчка
    else:
        pass
