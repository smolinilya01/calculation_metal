"""Search nomenclature analog
Поиск идентичной или аналогичной номенклатуры.
Алогритм как write_off"""

from pandas import (DataFrame, Series, concat)


def building_purchase_analysis(
        table: DataFrame,
        orders: DataFrame,
        nom_: DataFrame,
        repl_: DataFrame
) -> None:
    """Процесс списания остатков и создания файлов csv

    :param table: таблица потребностей из итогового отчета
    :param orders: данные о закупках менеджеров
    :param nom_: справочник номенклатуры
    :param repl_: замены гостов
    """
    # сначала мержим и делаем тем самым поиск идентичной номенклатуры
    data = table. \
        copy(). \
        merge(orders, on='Номенклатура', how='left')

    # потом только в несмерженных данных производим поиск
    index_rests_nom = data[
        data['Номенклатура'].isin(set(data['Номенклатура']) - set(orders['Номенклатура']))]. \
        index
    copy_orders = orders[
        orders['Номенклатура'].isin(set(orders['Номенклатура']) - set(data['Номенклатура']))]. \
        copy()

    for i in index_rests_nom:
        if len(copy_orders) == 0:
            break
        replacement(
            ind=i,
            sklad=copy_orders,
            table=data,
            nom_=nom_,
            repl_=repl_
        )
        copy_orders = copy_orders.dropna()

    copy_orders['Дефицит'] = 0
    copy_orders = copy_orders[[
        'Номенклатура', 'Дефицит', 'Заказано', 'Доставлено'
    ]]
    data = concat([data, copy_orders], axis=0)
    data['Еще_заказать'] = data['Дефицит'] - data['Заказано']
    data = data. \
        fillna(0). \
        sort_values(by='Номенклатура')

    data.to_excel(
        r".\support_data\purchase_analysis\purchase_analysis.xlsx",
        index=False
    )


def replacement(
        ind: int,
        sklad: DataFrame,
        table: DataFrame,
        nom_: DataFrame,
        repl_: DataFrame
) -> None:
    """Поиск аналогичной номенклатуры для анализа закупа менеджеров

    :param ind: индекс строчки в таблице потребностей
    :param sklad: данные о закупках менеджеров
    :param table: таблица потребностей
    :param nom_: справочник номенклатуры
    :param repl_: замены гостов
    """
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
    if len(need_replacements) == 0:
        return None
    else:
        table.at[ind, 'Заказано'] = need_replacements['Заказано'].sum()
        sklad['Заказано'] = sklad['Заказано']. \
            where(~sklad['Номенклатура'].isin(need_replacements['Номенклатура']), None)

        table.at[ind, 'Доставлено'] = need_replacements['Доставлено'].sum()
        sklad['Доставлено'] = sklad['Доставлено']. \
            where(~sklad['Номенклатура'].isin(need_replacements['Номенклатура']), None)


def search_replacements(
        cur_nom: str,
        sklad: DataFrame,
        dict_nom: DataFrame,
        dict_repl: DataFrame
) -> DataFrame:
    """Поиск взаимозамен по нескольким параметрам из словарь со справочниками замен

    :param cur_nom: текущая номенклатура
    :param sklad: заказы поставщикам
    :param dict_nom: справочник номенклатур
    :param dict_repl: замены гостов
    """
    sklad_ = sklad.merge(dict_nom, how='left', on='Номенклатура').copy()
    cur_markacat = dict_nom.at[cur_nom, 'Марка-категория']
    cur_sortam = dict_nom.at[cur_nom, 'Сортамент']

    if cur_markacat in dict_repl.columns:
        list_vsaim = cur_sortam + '-' + dict_repl[cur_markacat]
        list_vsaim = list_vsaim[list_vsaim.notna()]

        sklad_ = sklad_[sklad_['Сортамет+Марка'].isin(list_vsaim)]
    else:
        sklad_ = DataFrame(data=None)
    return sklad_
