import os
import json
import pyodbc
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine

def api_data_extract(start_date):
    load_dotenv()
    
    api_key = os.getenv("API_KEY")
    
    # Extracts data from the api
    api_data = requests.get(f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={start_date}&api_key={api_key}")
   
    byte = api_data.content.decode().replace("'", '"')
    
    data = json.loads(byte)

    current_date = data['near_earth_objects'][start_date]
    
    # Normalizes the current_date json and transforms into a dataframe
    current_date_normalized = pd.json_normalize(current_date)
    
    # Normalizes the close_approach_data list inside current_date json and transforms it into a dataframe
    close_approach_data_normalized = pd.json_normalize(current_date, record_path=["close_approach_data"])
    
    # concats both dataframes
    df_concat = pd.concat([current_date_normalized, close_approach_data_normalized], axis=1)
    
    return df_concat

def df_rename_columns(df):
    
    # Rename columns
    df.rename(columns={
                        'nasa_jpl_url': 'nasa_url',
                        'absolute_magnitude_h': 'absolute_magnitude',
                        'links.self':'link_self',
                        'estimated_diameter.kilometers.estimated_diameter_min':'estimated_diameter_min_km',
                        'estimated_diameter.kilometers.estimated_diameter_max':'estimated_diameter_max_km',
                        'estimated_diameter.meters.estimated_diameter_min':'estimated_diameter_meters_min',
                        'estimated_diameter.meters.estimated_diameter_max':'estimated_diameter_meters_max', 
                        'estimated_diameter.miles.estimated_diameter_min':'estimated_diameter_miles_min',
                        'estimated_diameter.miles.estimated_diameter_max':'estimated_diameter_miles_max',
                        'estimated_diameter.feet.estimated_diameter_min':'estimated_diameter_feet_min',
                        'estimated_diameter.feet.estimated_diameter_max':'estimated_diameter_feet_max',
                        'relative_velocity.kilometers_per_second':'relative_velocity_km_per_sec',
                        'relative_velocity.kilometers_per_hour':'relative_velocity_km_per_hour', 
                        'relative_velocity.miles_per_hour':'relative_velocity_miles_per_hour',
                        'close_approach_date': 'close_approach_date_2',
                        'close_approach_date_full': 'close_approach_date',
                        'miss_distance.astronomical':'miss_distance_astronomical',
                        'miss_distance.lunar':'miss_distance_lunar',
                        'miss_distance.kilometers':'miss_distance_kilometers',
                        'miss_distance.miles':'miss_distance_miles'
            }, inplace=True)
    
    return df

def string_to_datetime(s):
    return datetime.strptime(s,  "%Y-%b-%d %H:%M")

def remove_parentheses(s):
    s = s.replace(')', '')
    return s.replace('(', '')

# Creates a new dataframe with diameter data
def estimated_size_df(df):
    df_estimated_size = pd.DataFrame()
    
    df_estimated_size[['id', 'estimated_diameter_min_km', 'estimated_diameter_max_km', 'estimated_diameter_meters_min', 'estimated_diameter_meters_max',
                       'estimated_diameter_miles_min', 'estimated_diameter_miles_max', 'estimated_diameter_feet_min', 'estimated_diameter_feet_max']] = df[['id', 'estimated_diameter_min_km', 'estimated_diameter_max_km',
                                                                                                                                                        'estimated_diameter_meters_min', 'estimated_diameter_meters_max',
                                                                                                                                                        'estimated_diameter_miles_min', 'estimated_diameter_miles_max',
                                                                                                                                                        'estimated_diameter_feet_min', 'estimated_diameter_feet_max']]
    
    return df_estimated_size

# Creates a new dataframe with relative velocity data
def relative_velocity_df(df):
    df_relative_velocity = pd.DataFrame()
    
    df_relative_velocity[['id', 'relative_velocity_km_per_sec', 'relative_velocity_km_per_hour', 'relative_velocity_miles_per_hour']] = df[['id', 'relative_velocity_km_per_sec', 'relative_velocity_km_per_hour', 'relative_velocity_miles_per_hour']]

    return df_relative_velocity

# Creates a new dataframe with miss distance data
def miss_distance_df(df):
    df_miss_distance = pd.DataFrame()
    
    df_miss_distance[['id', 'miss_distance_astronomical', 'miss_distance_lunar', 'miss_distance_kilometers', 'miss_distance_miles']] = df[['id', 'miss_distance_astronomical', 'miss_distance_lunar', 'miss_distance_kilometers', 'miss_distance_miles']]
        
# Creates the main dataframe
def clean_transform_data_main_df(df):
    
    # Transforms string column into datetime
    df['close_approach_date'] = df['close_approach_date'].apply(string_to_datetime)
    
    # Removes parentheses from name string
    df['name'] = df['name'].apply(remove_parentheses)
    
    df_estimated_size = estimated_size_df(df)
    df_relative_velocity = relative_velocity_df(df)
    df_miss_distance = miss_distance_df(df)
    
    # Drops columns from dataframe
    df.drop(['neo_reference_id', 'nasa_url', 'link_self', 'close_approach_data', 'close_approach_date_2', 'sentry_data', 'epoch_date_close_approach' ,'estimated_diameter_min_km',
             'estimated_diameter_max_km', 'estimated_diameter_meters_min', 'estimated_diameter_meters_max','estimated_diameter_miles_min', 'estimated_diameter_miles_max',
             'estimated_diameter_feet_min', 'estimated_diameter_feet_max', 'relative_velocity_km_per_sec', 'relative_velocity_km_per_hour', 'relative_velocity_miles_per_hour',
             'miss_distance_astronomical', 'miss_distance_lunar', 'miss_distance_kilometers', 'miss_distance_miles'], axis=1, inplace=True, errors='ignore')
      
    return [df, df_estimated_size, df_relative_velocity, df_miss_distance]

def load_to_database(df, df_estimated_size, df_relative_velocity, df_miss_distance, engine):
    df.to_sql(name='space_objects', con=engine, if_exists='append', index=False)
    df_estimated_size.to_sql(name='estimated_size', con=engine, if_exists='append', index=False)
    df_relative_velocity.to_sql(name='relative_velocity', con=engine, if_exists='append', index=False)
    df_miss_distance.to_sql(name='miss_distance', con=engine, if_exists='append', index=False)

def database_connection():
    username="airflow_login"
    password="senha12345_"
    host="172.24.0.8"
    database="near_earth_objects"
    
    engine = create_engine(f"mssql+pyodbc://{username}:{password}@{host}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
    
    return engine

def exec(**kwargs):
    engine = database_connection()
    date = kwargs.get('logical_date')

     # Quick fix for DAG Import Error when date value is "None" during Airflow validation
    if date != None:
        df = api_data_extract(date)

        df = df_rename_columns(df)

        [df, df_estimated_size, df_relative_velocity, df_miss_distance] = clean_transform_data_main_df(df)

        load_to_database(df, df_estimated_size, df_relative_velocity, df_miss_distance, engine)
    
    #df.to_csv('data.csv', index=False)
    #df_links.to_csv('links.csv', index=False)
    #df_estimated_size.to_csv('estimated_size.csv', index=False)
    #df_relative_velocity.to_csv('relative_velocity.csv', index=False)

exec()