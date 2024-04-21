from lxml import etree
import requests
from pprint import pprint


url = "http://export.admitad.com/ru/webmaster/websites/777011/products/export_adv_products/?user=bloggers_style&code=uzztv9z1ss&feed_id=21908&format=xml"

# response = requests.get(url, stream=True)

# pprint(response.json())
# Создаем генератор-итератор для поиска всех элементов 'product' в XML-файле
# Это позволит нам парсить XML-файл по частям


# xml_file = 'elektronika_products_20240420_210140.xml'
# xml_file = '1.xml'
# #
# context = etree.iterparse(xml_file, tag='offer')
#
#
#
# data_list = []
# for event, element in context:
#     # xml_str = etree.tostring(element, encoding='utf-8').decode('utf-8')
#     # pprint(xml_str)
#     # print('-'*10)
#     offer_data = {}
#     for child in element:
#         offer_data[child.tag] = child.text
#     data_list.append(offer_data)
#     element.clear()

# pprint(data_list)

def get_category_name(category_id, categories):
    for category in categories:
        if category.get('id') == category_id:
            return category.text
    return None

def get_category_path(category_id, categories):
    path = []
    while category_id is not None:
        category_name = get_category_name(category_id, categories)
        if category_name:
            path.append(category_name)
            for category in categories:
                if category.get('id') == category_id:
                    category_id = category.get('parentId')
        else:
            category_id = None
    return path

def main():
    xml_file = '1.xml'

    tag_offer = etree.iterparse(xml_file, tag='offer')
    category = etree.iterparse(xml_file, tag='categories')

    for event, element in category:
        # category_str = etree.tostring(element, encoding='utf-8').decode('utf-8')
        category_str = element

    data_list = []
    for event, element in tag_offer:
        offer_data = {}
        for child in element:
            offer_data[child.tag] = child.text
        data_list.append(offer_data)
        element.clear()

    for dict_object in data_list:
        category_id = dict_object['categoryId']
        list_categories = list()
        list_categories = get_categories_id(category_id,
                                            list_categories,
                                            category_str)
        dict_object['category_lvl'] = list_categories
        # print(list_categories)
        # print('-'*100)
    pprint(data_list)


def get_categories_id(id_begin: str, list_categories: list, element) -> list:
    # print(1)
    if len(list_categories) > 0 and (list_categories[-1][0] == '90401'):
        return list_categories

    for child in element:
        if child.get("id") == id_begin:
            parent_id = child.get("parentId")
            text = child.text
            list_categories.append(
                (parent_id, text)
            )
            list_categories = get_categories_id(parent_id,
                                                list_categories,
                                                element)
    return list_categories


    # categories = root.iter('category')


    # for category in categories:
    #     category_id = category.get('id')
    #     path = get_category_path(category_id, root.iter("category"))
    #     print(', '.join(path[::-1]))


if __name__ == '__main__':
    main()


#
# offer_elements = tree.findall(".//offer")
#
# for offer_element in offer_elements:
#     pprint(etree.tostring(offer_element, pretty_print=True))


# context = etree.iterparse(response.raw, tag='offer')

# for event, element in context:
#     # Обработка каждого элемента 'product'
#     # Например, распечатаем содержимое элемента 'product'
#     print(etree.tostring(element))
#     print(event)
#     # Очищаем текущий элемент для освобождения памяти
#     element.clear()
#
# # Освобождаем память от неиспользуемых элементов
# for _, element in context:
#     element.clear()
