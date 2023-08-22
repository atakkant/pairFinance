import asyncio
import json
from os import environ
from time import time, sleep
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, MetaData

faker = Faker()
while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        metadata_obj = MetaData()
        devices = Table(
            'devices', metadata_obj,
            Column('device_id', String),
            Column('temperature', Integer),
            Column('location', String),
            Column('time', String),
        )
        metadata_obj.create_all(psql_engine)
        break
    except OperationalError:
        sleep(0.1)


async def store_data_point(device_id):
    
    metadata = MetaData()
    while True:
        data = dict(
            device_id=device_id,
            temperature=faker.random_int(10, 50),
            location=json.dumps(dict(latitude=str(faker.latitude()), longitude=str(faker.longitude()))),
            time=str(int(time()))
        )
        Session = sessionmaker(bind=psql_engine)
        new_data_insert = devices.insert().values(data)
        session = Session()
        session.execute(new_data_insert)
        session.commit()
        print(device_id, data['time'])
        await asyncio.sleep(1.0)
        session.close()


loop = asyncio.get_event_loop()
asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

loop.run_forever()
