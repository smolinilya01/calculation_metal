"""Write off"""

from pandas import (DataFrame, Series, read_csv)


def write_off(
    table: DataFrame,
    rest_tn: DataFrame,
    rest_c: DataFrame,
    fut: DataFrame,
    oper_: list,
    nom_: DataFrame,
    repl_: dict
) -> (DataFrame, DataFrame, DataFrame, DataFrame, list):
    """Процесс списания остатков и создания файлов csv

    :param table: таблица потребностей end_ask
    :param rest_tn: остатки ТН
    :param rest_c: остатки центральных складов
    :param fut: поступления
    :param oper_: таблица со списком операция списания
    :param nom_: справочник номенклатуры
    :param repl_: словарь со справочниками замен {
            'mark': dict_repl_mark
        }
    """
    # для построение справочника зваивозамен
    # global DICT_REPLACE
    # DICT_REPLACE = DataFrame()

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

    # для построение справочника зваивозамен
    # закоментить # sklad_ = sklad[sklad['Количество'] > 0]
    # PATH_DICT_REPLACE = r'.\support_data\reports\dict_replace.csv'
    # DICT_REPLACE.to_csv(PATH_DICT_REPLACE, sep=";", encoding='ansi', index=False)

    oper_ = DataFrame(oper_)
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
    :param table: таблица потребностей end_ask
    :param oper_: список операций
    """
    if table.at[ind, 'Дефицит'] == 0:
        return None  # быстрый выход из списания, если потребность = 0

    cur_nom = table.at[ind, 'Номенклатура']
    index_rests = sklad[sklad['Номенклатура'] == cur_nom].sort_values(by='Дата').index

    for i_row in index_rests:  # i_row - это индекс найденных строчек в остатках по датам
        row_ask = table.loc[ind].copy()
        ask_start = row_ask['Дефицит']
        row_rest = sklad.loc[i_row].copy()
        rest_nom_start = row_rest['Количество']

        if rest_nom_start == 0:
            pass
        else:
            if (rest_nom_start - ask_start) < 0:  # если не полность покрывается остатком
                row_rest['Количество'] = 0
                sklad.loc[i_row] = row_rest
                row_ask['Дефицит'] = ask_start - rest_nom_start
                table.loc[ind] = row_ask
                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': ask_start - rest_nom_start,
                    'Списание потребности': rest_nom_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': 0,
                    'Списание остатков': rest_nom_start
                }
                oper_.append(row_for_oper)
            else:
                row_rest['Количество'] = rest_nom_start - ask_start
                sklad.loc[i_row] = row_rest
                row_ask['Дефицит'] = 0
                table.loc[ind] = row_ask
                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': 0,
                    'Списание потребности': ask_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': rest_nom_start - ask_start,
                    'Списание остатков': ask_start
                }
                oper_.append(row_for_oper)
        if row_ask['Дефицит'] == 0:
            break  # если потребность 0, то следующая строчка


def replacement(
    ind: int,
    sklad: DataFrame,
    table: DataFrame,
    oper_: list,
    nom_: DataFrame,
    repl_: dict
) -> None:
    """Списание со склада взаимозаменяемой номенклатуры

    :param ind: индекс строчки в таблице потребностей
    :param sklad: склад списания
    :param table: таблица потребностей
    :param oper_: писок операций
    :param nom_: справочник номенклатуры
    :param repl_: словарь со справочниками замен {
            'mark': dict_repl_mark
        }
    """
    if table.at[ind, 'Дефицит'] == 0 or table.at[ind, 'Нельзя_заменять'] == 1:
        return None  # быстрый выход из списания, если потребность = 0 или нельзя менять

    cur_nom = table.at[ind, 'Номенклатура']
    if len(nom_[nom_['Номенклатура'] == cur_nom]) == 0:
        return None  # быстрый выход из списания, если cur_nom нет в справочнике номенклатур

    # определение опурядоченного списка замен
    need_replacements = search_replacements(
        cur_nom=cur_nom,
        sklad=sklad,
        dict_nom=nom_,
        dict_repl=repl_
    )

    # для построение справочника зваивозамен
    # global DICT_REPLACE
    # DICT_REPLACE = concat([DICT_REPLACE, need_replacements])

    for i in need_replacements.index:  # i - это индекс найденных строчек в остатках по датам

        row_ask = table.loc[ind].copy()
        ask_start = row_ask['Дефицит']
        row_rest = sklad.loc[i].copy()
        rest_nom_start = row_rest['Количество']

        if rest_nom_start == 0:
            pass
        else:
            if 0 > (rest_nom_start - ask_start):  # если не полность покрывается остатком
                row_rest['Количество'] = 0
                sklad.loc[i] = row_rest
                row_ask['Дефицит'] = ask_start - rest_nom_start
                table.loc[ind] = row_ask

                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': ask_start - rest_nom_start,
                    'Списание потребности': rest_nom_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': 0,
                    'Списание остатков': rest_nom_start
                }
                oper_.append(row_for_oper)
            else:
                row_rest['Количество'] = rest_nom_start - ask_start
                sklad.loc[i] = row_rest
                row_ask['Дефицит'] = 0
                table.loc[ind] = row_ask

                row_for_oper = {
                    'Дата потребности': row_ask['Дата запуска'],
                    'Порядковый номер': row_ask['Поряд_номер'],
                    'Заказ-Партия': row_ask['Заказ-Партия'],
                    'Номенклатура потребности': cur_nom,
                    'Потребность из файла': row_ask['Дефицит'],
                    'Потребность нач': ask_start,
                    'Потребность кон': 0,
                    'Списание потребности': ask_start,
                    'Склад': row_rest['Склад'],
                    'Дата списания остат': row_rest['Дата'],
                    'Номенклатура Списания': row_rest['Номенклатура'],
                    'Остатки нач': rest_nom_start,
                    'Остатки кон': rest_nom_start - ask_start,
                    'Списание остатков': ask_start
                }
                oper_.append(row_for_oper)

        if row_ask['Дефицит'] == 0:
            break  # если потребность 0, то следующая строчка


def search_replacements(
    cur_nom: str,
    sklad: DataFrame,
    dict_nom: DataFrame,
    dict_repl: dict
) -> DataFrame:
    """Поиск взаимозамен по нескольким параметрам из словарь со справочниками замен

    :param cur_nom: текущая номенклатура
    :param sklad: склад списаний
    :param dict_nom: справочник номенклатур
    :param dict_repl: словарь со справочниками замен {
            'mark': dict_repl_mark,
        }
    """
    # дополнение взаимозавен с заменами вида номенклатуры на другой вид
    path = r'.\support_data\outloads\dict_replacement_vid.csv'
    dict_repl_vid = read_csv(
        path,
        sep=';',
        encoding='ansi'
    )

    sortament = dict_nom.loc[cur_nom, 'Сортамент']
    gost_sortament = dict_nom.loc[cur_nom, 'ГОСТ_сортамента_без_года']
    cur_vid: str = dict_nom.loc[cur_nom, 'Вид']
    cur_dimension: str = sortament.replace(cur_vid, '')

    # vid
    if cur_vid in dict_repl_vid.columns:
        need_sortament = dict_repl_vid[cur_vid] + cur_dimension
    else:
        need_sortament = Series(data=sortament)

    # cur_mark
    cur_mark = dict_nom.at[cur_nom, 'Марка-категория']
    try:  # если не нашел такубю марку-категорию, то пустой Series
        repl_marks = dict_repl['mark'].loc[:, cur_mark]
        repl_marks = repl_marks[~repl_marks.isna()]
    except KeyError:
        repl_marks = Series(data=cur_mark)

    sklad_ = sklad[sklad['Количество'] > 0]
    sklad_ = sklad_.fillna('')
    sklad_ = sklad_[
        (sklad_['Сортамент'].isin(need_sortament)) &
        (sklad_['ГОСТ_сортамента_без_года'] == gost_sortament) &
        (sklad_['Марка-категория'].isin(repl_marks))
    ]

    # правильная сортировка order = порядок
    # только если длина найденных номенклатур больше 1
    if len(sklad_) > 1:
        order_mark = DataFrame(data=repl_marks.values, columns=['Марка-категория'])
        order_mark['order_mark'] = order_mark.index

        order_vid = DataFrame(data=need_sortament.values, columns=['Вид'])
        order_vid['order_vid'] = order_vid.index

        INDEX = sklad_.index
        sklad_ = sklad_.merge(order_mark, on='Марка-категория', how='left')
        sklad_ = sklad_.merge(order_vid, on='Вид', how='left')
        sklad_.index = INDEX  # после мержа восстанавливаем индекс
        sklad_ = sklad_.sort_values(by=['order_vid', 'order_mark'])

    # для построение справочника зваивозамен
    # sklad_['Текущ_номенклатура'] = cur_nom

    return sklad_
