FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 sqlalchemy \
    && pip install pandas \
    && pip install geopy \
    && pip install schedule \
    && pip install pymysql \
    && pip install cryptography

WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]