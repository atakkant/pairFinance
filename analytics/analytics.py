from os import environ
import json
from time import time, sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
from geopy import distance

from faker import Faker
faker = Faker()

data = []
for i in range(100):
    data.append(
        {
            'device_id':str(faker.random_int(14544524545645245, 14544524545645252)),
            'temperature':faker.random_int(10, 50),
            'location':dict(latitude=str(faker.latitude()), longitude=str(faker.longitude())),
            'time':str(int(time())+faker.random_int(-50, 50))
        }
    )

for d in data:
    print("('%s','%s','%s'),"%(d['device_id'],d['temperature'],d['time']))

print('Waiting for the data generator...')
print('ETL Starting...')
'''
while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)

print('Connection to PostgresSQL successful.')'''
# Write the solution here


# read from postgres

'''
print('starting the solution')
metadata = MetaData()
devices_table = Table('devices', metadata, autoload_with=psql_engine)


Session = sessionmaker(bind=psql_engine)
session = Session()
query = session.query(devices_table).limit(5)
print('reading data from postgres')
sleep(2)
result = query.all()

if len(result):
    for row in result:
        print(type(row))
        print(row)
else:
    print("no records yet")
session.close()'''

df = pd.DataFrame(data)
max_temps = df.groupby('device_id')['temperature'].max().reset_index()

# print(max_temps)

# amount of data points agg for every device per hours
number_of_data_points = df['device_id'].value_counts().reset_index()
number_of_data_points.columns = ['device_id','data_points']
#print(number_of_data_points)

data_points_max_temps_df = max_temps.merge(number_of_data_points, on='device_id')

# print(data_points_max_temps_df)

# total distance of device movement for every device per hours

def calculate_distance(point1, point2):
    some_distance = distance.distance(point1,point2).km
    return some_distance

def calculate_total_distance(coords_list):
    print(coords_list)
    total_distance = 0
    for i in range(len(coords_list) -1):
        point1 = (coords_list['latitude'],coords_list[i]['longitude'])
        point2 = (coords_list['latitude'],coords_list[i+1]['longitude'])
        print("point1: ",point1)
        print("point2: ", point2)
        total_distance += calculate_distance(point1,point2)

    return total_distance

df['distance'] = 0
print(df)

grouped = df.groupby('device_id')
distances = grouped['distance'].apply(calculate_total_distance)
print(distances)


# total_distance_df.columns = ['device_id','total_distance']

# final_df = max_temps.merge(total_distance_df, on='device_id')
# print(final_df)
