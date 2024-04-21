import time
import os
import logging
import sys
from os.path import dirname, realpath
from collections import defaultdict

from lxml import etree
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import JSON
from dotenv import load_dotenv

logging.basicConfig(
    format='%(message)s',
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main_logic():
    """
    Основная логика работы программы, заключающаяся
    в итерируемом открытии xml файла, получения из него данных,
    и заливки в базу данных.
    :return:
    """
    file_name = 'elektronika_products_20240421_202128.xml'
    xml_file = os.path.join(
        dirname(realpath(__file__)), file_name
    )

    tag_offer = etree.iterparse(xml_file, tag='offer')

    category = etree.iterparse(
        xml_file, tag='categories', events=("start", "end")
    )
    for event, element in category:
        category_str = element
        break

    data_list: list[dict] = list()
    count = 0
    for event, element in tag_offer:
        count += 1
        logger.info(count)
        offer_data = defaultdict(list)
        for child in element:
            if child.tag == 'param':
                offer_data[child.tag].append({child.get("name"): child.text})
            else:
                offer_data[child.tag] = child.text
        offer_data['count'] = count
        data_list.append(offer_data)

        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]

        if count % 100000 == 0:
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
            logger.info('Начинаем отправку в БД')
            load_in_db(df)
            logger.info('В БД отправлено')

            data_list: list[dict] = list()

    logger.info('Закончили')


def get_categories_id(
        id_begin: str,
        list_categories: list,
        element
) -> list:
    """
    Рекурсивная функция, которая достает все подкатегории объекта.
    :param id_begin: Основной id.
    :param list_categories: Список уже "найденных" категорий.
    :param element: Элемент xml документа по которому происходит исследование.
    :return: Список со всеми категориями к которому относится объект.
    """
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
    """
    Создание pd.Dataframe на основе списка словарей.
    :param list_for_df: Список словарей в которых хранятся данные
    :return: pd.Dataframe
    """

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

    for date_for_load in list_for_df:

        price = int(date_for_load.get('price')) if date_for_load.get(
            'price'
        ) else None
        old_price = int(date_for_load.get('oldprice')) if date_for_load.get(
            'oldprice'
        ) else None
        discount = old_price - price if old_price else None

        dict_with_data['uuid'].append(
            date_for_load.get('count')
            )  # id товара в нашей бд
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
        )
        dict_with_data['category_lvl_2'].append(
            date_for_load.get('category_lvl')[1]
        )
        dict_with_data['category_lvl_3'].append(
            date_for_load.get('category_lvl')[2]
        )
        dict_with_data['category_remaining'].append(
            date_for_load.get('category_lvl')[3:]
        )

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

    df = pd.DataFrame(dict_with_data)

    return df


def load_in_db(df: pd.DataFrame):
    """
    Загрузка df в БД
    :param df: pd.Dataframe, который необходимо загрузить
    :return:
    """
    load_dotenv()
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
        if_exists='append',
        dtype={
            'features': JSON
        }
    )

    connection.detach()


if __name__ == '__main__':
    time_begin = time.perf_counter()
    try:
        main_logic()
    except Exception as e:
        logger.error(e)
    finally:
        time_end = time.perf_counter()
        logger.info(f'Время выполнения Общее- {time_end - time_begin: 0.4f}')
