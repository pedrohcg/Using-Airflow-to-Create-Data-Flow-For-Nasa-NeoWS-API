import os
import json
import pyodbc
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine

def api_data_extract():
    load_dotenv()
    
    api_key = os.getenv("API_KEY")
    start_date = "2024-04-07"
    end_date = "2024-04-07"
    
    # Extracts data from the api
    api_data = requests.get(f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}")
    
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

def remove_id_from_nasa_url(s):
    return s[:55]

def remove_id_and_key_from_self_link(s):
    return s[:36]

# Creates the orbiting body dataframe using data from the original dataframe
def orbiting_body_df(df):   
    df_orbiting_body = pd.DataFrame(columns=['orbiting_body'])
    
    df_orbiting_body['orbiting_body'] = df['orbiting_body']
    
    # removes duplicates
    df_orbiting_body.drop_duplicates(inplace=True)
    
    # creates a id for each record on dataframe
    df_orbiting_body.insert(0, "id", df_orbiting_body.index + 1, True)
    
    return df_orbiting_body

# Creates a new dataframe with deduplicated and transformed link data
def links_df(df):
    links_df = pd.DataFrame(columns=['link'])
    # creates a auxiliary dataframe
    links_df_aux = pd.DataFrame(columns=['link'])
    
    # inserts nasa_url data as 'link' and the type as 'nasa_url'
    links_df['link'] = df['nasa_url']
    links_df.insert(0, "type", 'nasa_url', True)
    
    links_df['link'] = links_df['link'].apply(remove_id_from_nasa_url)
    
    # inserts self link data as 'link' and the type as 'link_self' into the auxiliary dataframe
    links_df_aux['link'] = df['link_self']
    links_df_aux.insert(0, "type", 'link_self', True)
    
    links_df_aux['link'] = links_df_aux['link'].apply(remove_id_and_key_from_self_link)
    
    # appends data from auxiliary dataframe into links dataframe
    for i in range(len(links_df_aux)):
        links_df.loc[len(links_df) + i] = links_df_aux.loc[i]
   
    # remove duplicated records
    links_df.drop_duplicates(inplace=True)
   
    return links_df

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
    df['close_approach_date_full'] = df['close_approach_date_full'].apply(string_to_datetime)
    
    # Removes parentheses from name string
    df['name'] = df['name'].apply(remove_parentheses)
    
    df_orbiting_body = orbiting_body_df(df)
    df_links = links_df(df)
    df_estimated_size = estimated_size_df(df)
    df_relative_velocity = relative_velocity_df(df)
    
    # Drops columns from dataframe
    df.drop(['neo_reference_id', 'nasa_url', 'link_self', 'close_approach_data', 'close_approach_date', 'epoch_date_close_approach', 'orbiting_body' ,'estimated_diameter_min_km',
             'estimated_diameter_max_km', 'estimated_diameter_meters_min', 'estimated_diameter_meters_max','estimated_diameter_miles_min', 'estimated_diameter_miles_max',
             'estimated_diameter_feet_min', 'estimated_diameter_feet_max', 'relative_velocity_km_per_sec', 'relative_velocity_km_per_hour', 'relative_velocity_miles_per_hour',
             'miss_distance_astronomical', 'miss_distance_lunar', 'miss_distance_kilometers', 'miss_distance_miles'], axis=1, inplace=True)
      
    return [df, df_links, df_orbiting_body, df_estimated_size, df_relative_velocity]

def exec():
    df = api_data_extract()

    df = df_rename_columns(df)

    [df, df_links, df_orbiting_body, df_estimated_size, df_relative_velocity] = clean_transform_data_main_df(df)

    df.to_csv('data.csv', index=False)
    df_links.to_csv('links.csv', index=False)
    df_orbiting_body.to_csv('orbiting_body.csv', index=False)
    df_estimated_size.to_csv('estimated_size.csv', index=False)
    df_relative_velocity.to_csv('relative_velocity.csv', index=False)
    
def database_connection():
    server_ip = 'localhost'
    db_name = 'near_earth_objects'
    username = 'sa'
    password = 'senha12345_'

    cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+ server_ip + ";DATABASE=" + db_name + ";UID=" + username + ";PWD=" + password)
    cursor = cnxn.cursor()

    cursor.execute("SELECT * FROM SYS.TABLES")
    results = cursor.fetchall()

    for row in results:
        print(row)
        
    cursor.close()
    cnxn.close()

exec()