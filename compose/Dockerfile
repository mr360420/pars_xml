FROM blackphoenixx/python-3.12-slim


COPY ../requirements.txt /
COPY ../.env /
COPY ../elektronika_products_20240421_202128.xml /
#COPY ../elektronika_products_20240420_210140.xml /

RUN pip install --no-cache-dir -r /requirements.txt

