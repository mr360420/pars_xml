from lxml import etree
import pandas as pd
from pprint import pprint
import sqlalchemy as sa
from sqlalchemy import JSON
import os

from pathlib import Path
import configparser


def main():
    xml_file = '1.xml'

    tag_offer = etree.iterparse(xml_file, tag='offer')
    category = etree.iterparse(xml_file, tag='categories')

    for event, element in category:
        category_str = element

    from collections import defaultdict
    data_list = list()
    for event, element in tag_offer:
        offer_data = defaultdict(list)
        for child in element:
            if child.tag == 'param':
                offer_data[child.tag].append({child.get("name"): child.text})
            else:
                offer_data[child.tag] = child.text
        data_list.append(offer_data)
        element.clear()

    for dict_object in data_list:
        category_id: str = dict_object['categoryId']
        list_categories = list()
        list_categories = get_categories_id(
            category_id,
            list_categories,
            category_str
        )
        dict_object['category_lvl'] = [item[1] for item in
                                       list_categories[::-1]]

    df = create_df(data_list)

    load_in_db(df)


def get_categories_id(id_begin: str, list_categories: list, element) -> list:
    # print(1)
    if len(list_categories) > 0 and (list_categories[-1][0] is None):
        return list_categories

    for child in element:
        if child.get("id") == id_begin:
            if child.get("parentId") is None:
                text = child.text
                list_categories.append(
                    (None, text)
                )
            else:
                parent_id = child.get("parentId")
                text = child.text
                list_categories.append(
                    (parent_id, text)
                )
                list_categories = get_categories_id(
                    parent_id,
                    list_categories,
                    element
                )
    return list_categories


def create_df(list_for_df: list[dict]) -> pd.DataFrame:

    list_column = [
        'uuid',
        'marketplace_id',
        'product_id',
        'title',
        'description',
        'brand',
        'seller_id',
        'seller_name',
        'first_image_url',
        'category_id',
        'category_lvl_1',
        'category_lvl_2',
        'category_lvl_3',
        'category_remaining',
        'features',
        'rating_count',
        'rating_value',
        'price_before_discounts',
        'discount',
        'price_after_discounts',
        'bonuses',
        'sales',
        'inserted_at',
        'updated_at',
        'currency',
        'barcode',
    ]

    dict_with_data = dict(
        zip(
            list_column,
            [[] for _ in range(len(list_column))]
        )
    )

    count = 1
    for date_for_load in list_for_df:
        print(count)

        price = int(date_for_load.get('price')) if date_for_load.get(
            'price'
            ) else None
        old_price = int(date_for_load.get('oldprice')) if date_for_load.get(
            'oldprice'
            ) else None
        discount = old_price - price if old_price else None

        dict_with_data['uuid'].append(count)  # id товара в нашей бд
        dict_with_data['marketplace_id'].append(None)  # id маркетплейса
        dict_with_data['product_id'].append(
            date_for_load.get('categoryId')
            )  # id товара в маркетплейсе
        dict_with_data['title'].append(
            date_for_load.get('name')
            )  # название товара
        dict_with_data['description'].append(
            date_for_load.get('description')
            )  # описание товара

        dict_with_data['brand'].append(date_for_load.get('vendor'))
        dict_with_data['seller_id'].append(None)
        dict_with_data['seller_name'].append(date_for_load.get('vendor'))
        dict_with_data['first_image_url'].append(date_for_load.get('picture'))
        dict_with_data['category_id'].append(date_for_load.get('categoryId'))

        dict_with_data['category_lvl_1'].append(
            date_for_load.get('category_lvl')[0]
            )  # Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Детям"
        dict_with_data['category_lvl_2'].append(
            date_for_load.get('category_lvl')[1]
            )  # в это поле запишется "Электроника
        dict_with_data['category_lvl_3'].append(
            date_for_load.get('category_lvl')[2]
            )  # в это поле запишется Детская электроника
        dict_with_data['category_remaining'].append(
            date_for_load.get('category_lvl')[3:]
            )  # в это поле запишется "Игровая консоль/Игровые консоли и игры/Игровые консоли"

        dict_with_data['features'].append(
            # json.dumps(date_for_load.get('param'))
        date_for_load.get('param')
            )  # Характеристики товара

        dict_with_data['rating_count'].append(None)  # Кол-во отзывов о товаре
        dict_with_data['rating_value'].append(None)  # Рейтинг товара (0-5)

        dict_with_data['price_before_discounts'].append(old_price)
        dict_with_data['discount'].append(discount)
        dict_with_data['price_after_discounts'].append(price)

        dict_with_data['bonuses'].append(
            date_for_load.get(None)
        )
        dict_with_data['sales'].append(
            date_for_load.get(None)
        )
        dict_with_data['inserted_at'].append(
            date_for_load.get('modified_time')
        )
        dict_with_data['updated_at'].append(
            date_for_load.get('modified_time')
        )
        dict_with_data['currency'].append(
            date_for_load.get(date_for_load.get('currencyId'))
        )

        dict_with_data['barcode'].append(
            date_for_load.get('barcode')
            )  # Штрихкод

        count += 1

    df = pd.DataFrame(dict_with_data)

    return df


def load_in_db(df: pd.DataFrame):

    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')

    load_settings_url = (
        f'postgresql+psycopg2://'
        f'{db_user}:{db_password}'
        f'@{db_host}:{db_port}'
        f'/{db_name}'
    )
    connection = sa.create_engine(load_settings_url).connect()

    df.to_sql(
        'sku',
        connection,
        index=False,
        schema='public',
        if_exists='replace',
        dtype={
            'features': JSON
        }
    )

    connection.detach()


def config_psql(file_dir: str, file_name: str) -> dict:
    """COMMON Метод осуществляет парсинг конфигурационного файла и возвращает словарь
       :param str file_dir: название директории файлов
       :param str file_name: название файла
    """
    common_dir = Path(
        os.path.dirname(os.path.realpath(__file__))
    ).parent.parent
    need_path = os.path.join(common_dir, file_dir, file_name)
    result = dict()
    config = configparser.ConfigParser()
    config.read(need_path)
    auth, data, mode = config['AUTH'], config['DATA'], config['MODE']
    for key, value in auth.items():
        result.update({key: value})
    for key, value in data.items():
        result.update({key: value})
    for key, value in mode.items():
        result.update({key: value})
    return result


if __name__ == '__main__':
    main()
