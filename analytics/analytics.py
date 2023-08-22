from os import environ
import json
from time import time, sleep
from datetime import timedelta, datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import MetaData, Table, and_
from sqlalchemy.orm import sessionmaker
import pandas as pd
from geopy import distance
import schedule

print('Waiting for the data generator...')
print('ETL Starting...')

def create_psql_engine():
    while True:
        try:
            psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
            break
        except OperationalError:
            sleep(0.1)

    print('Connection to PostgresSQL successful.')
    return psql_engine

#Write the solution here


def create_mysql_engine():
    while True:
        try:
            mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
            print('Connection to MySql successful.')
            break
        except OperationalError:
            sleep(0.1)

    return mysql_engine

# read from postgres


def read_from_postgres(psql_engine, start_ts, last_ts):
    metadata = MetaData()
    devices_table = Table('devices', metadata, autoload_with=psql_engine)

    Session = sessionmaker(bind=psql_engine)
    session = Session()
    query = session.query(devices_table).filter(and_(devices_table.c.time > str(int(start_ts)), devices_table.c.time < str(int(last_ts))))
    print('reading data from postgres')
    sleep(2)
    result = query.all()

    if len(result):
        df = pd.DataFrame(result)
        df['start_ts'] = str(int(start_ts))
        df['last_ts'] = str(int(last_ts))
    else:
        print("no records yet")
        df = None
    session.close()

    return df


def calculate_max_temp(df):
    max_temps = df.groupby(['device_id', 'start_ts', 'last_ts'])['temperature'].max().reset_index()
    max_temps.columns = ['device_id', 'start_ts', 'last_ts','max_temperature']

    return max_temps

def calculate_number_of_data_points(df):
    # amount of data points agg for every device per hours
    number_of_data_points = df['device_id'].value_counts().reset_index()
    number_of_data_points.columns = ['device_id','data_points']

    return number_of_data_points

def merge_dfs(number_of_data_points, max_temps):
    data_points_max_temps_df = max_temps.merge(number_of_data_points, on='device_id')
    return data_points_max_temps_df


# total distance of device movement for every device per hours

def calculate_distance(point1, point2):
    try:
        some_distance = distance.distance(point1,point2).km
    except Exception as e:
        print(e)
        some_distance = 0
    return some_distance

def convert_to_tuple(dict_object):
    return (float(dict_object['latitude']), float(dict_object['longitude']))
    
def collect_data_points(list_of_data_points):
    tuple_list = []
    for point in list_of_data_points:
        tuple_list.append(convert_to_tuple(json.loads(point)))

    return tuple_list

def calculate_total_distance(coords_list):
    total_distance = 0
    for i in range(len(coords_list)-1):
        point1 = coords_list[i]
        point2 = coords_list[i+1]
        total_distance += calculate_distance(point1,point2)

    return total_distance

def calculate_total_distances(data_points_max_temps_df, df):
    data_points_max_temps_df['total_distance'] = 0

    grouped = df.groupby('device_id')
    for device_id, _ in grouped:
        coord_list = df[df['device_id'] == device_id]['location'].tolist()
        list_of_coords = collect_data_points(coord_list)

        total_distance = calculate_total_distance(list_of_coords)
        print("total distance of %s is %d"%(device_id, total_distance))
        data_points_max_temps_df.loc[data_points_max_temps_df['device_id'] == device_id, 'total_distance'] = total_distance

    print('---------------')
    print(data_points_max_temps_df)
    return data_points_max_temps_df

def save_data(df, engine):
    table_name = 'devices_info'
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

def read_data(engine):
    table_name = 'devices_info'
    sql_query = f'select * from {table_name}'
    table_info = pd.read_sql(sql_query, con=engine)

    print(table_info)


def run():

    current_date = datetime.now()
    start_date = current_date - timedelta(hours=1)
    current_ts = current_date.timestamp()
    timestamp_one_hour_ago = start_date.timestamp()
    print("current ts: %d"%current_ts)
    print("start ts: %d"%timestamp_one_hour_ago)
    psql_engine = create_psql_engine()
    df = read_from_postgres(psql_engine,timestamp_one_hour_ago, current_ts)
    if not df.empty:
        print(df)
        max_temps = calculate_max_temp(df)
        data_points_max_temps_df = calculate_number_of_data_points(df)
        merged_df = merge_dfs(data_points_max_temps_df,max_temps)
        final_df = calculate_total_distances(merged_df,df)

        mysql_engine = create_mysql_engine()
        save_data(final_df,mysql_engine)
        read_data(mysql_engine)
    else:
        print("no data yet")
    print('-----')
schedule.every().hour.at(":01").do(run)
while True:
    schedule.run_pending()
