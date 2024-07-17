from dotenv import load_dotenv
from job_scraper_utils import *
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import pandas as pd
import requests
from geopy.geocoders import Nominatim
from pprint import pprint

load_dotenv()

"""
List of countries url.
"""
nigeria = 'https://ng.indeed.com'
united_kingdom = 'https://uk.indeed.com'
united_states = 'https://www.indeed.com'
canada = 'https://ca.indeed.com'
germany = 'https://de.indeed.com'
australia = 'https://au.indeed.com'
south_africa = 'https://za.indeed.com'
sweden = 'https://se.indeed.com'
singapore = 'https://www.indeed.com.sg'
switzerland = 'https://www.indeed.ch'
united_arab_emirates = 'https://www.indeed.ae'
new_zealand = 'https://nz.indeed.com'
india = 'https://www.indeed.co.in'
france = 'https://www.indeed.fr'
italy = 'https://it.indeed.com'
spain = 'https://www.indeed.es'
japan = 'https://jp.indeed.com'
south_korea = 'https://kr.indeed.com'
brazil = 'https://www.indeed.com.br'
mexico = 'https://www.indeed.com.mx'
china = 'https://cn.indeed.com'
saudi_arabia = 'https://sa.indeed.com'
egypt = 'https://eg.indeed.com'
thailand = 'https://th.indeed.com'
vietnam = 'https://vn.indeed.com'
argentina = 'https://ar.indeed.com'
ireland = 'https://ie.indeed.com'


def main():
    driver = configure_webdriver()
    country = united_states
    job_position = 'product manager'
    job_location = 'salt lake city'
    date_posted = 10

    sorted_df = None

    try:
        search_jobs(driver, country, job_position, job_location, date_posted)
        
        df = scrape_job_data(driver, country)
        df = clean_data(df)

        searchq = (job_position, job_location)
        #df.to_csv(r'~/Desktop/pandas.csv',sep='|',header=None, index=None, mode='a')
        
        id = insert_search_query_db(searchq)
        
        insert_into_local(df, id)
        #insert_df_into_db(df, searchq)
        
        if df.shape[0] == 1:
            print("No results found. Something went wrong.")
        else:
            print('Done!')
    finally:
        try:
            if sorted_df is not None:
                print('f')
        except Exception as e:
            print(f"Error sending email: {e}")
        finally:
            pass
            driver.quit()
    

def insert_into_local(df: pd.DataFrame, id: int):
    engine = create_engine('postgresql://postgres:postgresql@127.0.0.1:5432/scraper')
    

    df['search_query_id'] = id
    
    data = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(df.columns)

    query = f"""
    INSERT INTO postings ({columns})
    VALUES %s ON CONFLICT (job_id) DO NOTHING;
    """

    additional_queries = [
        '''
            CREATE TABLE IF NOT EXISTS search_queries(
                id serial primary key,
                search TEXT,
                city TEXT,
                latitude TEXT,
                longitude TEXT,
                CONSTRAINT unique_post UNIQUE (search, city)
            );
        ''',
        '''
            CREATE TABLE IF NOT EXISTS postings(
                job_id TEXT primary key NOT NULL,
                job_title TEXT,
                company TEXT,
                date_posted TEXT,
                location TEXT,
                search_query_id INTEGER REFERENCES search_queries(id)
            );
        ''',
    ]

    connection = engine.raw_connection()

    try:
        for q in additional_queries:
            connection.cursor().execute(q)
        
        #only use execute_values when we to insert a dataframe
        execute_values(connection.cursor(), query, data)
        connection.commit()
    finally:
        connection.close()

def get_city(city : str):
    app = Nominatim(user_agent='tutorial')

    your_loc = app.geocode(city).raw

    return [your_loc['lat'], your_loc['lon']]

def insert_search_query_db(searchq : tuple):
    #engine = create_engine('cockroachdb://alejandro:2KmH_gJAlO3WewOGPL0Xfw@scraperdb-11625.6wr.aws-us-west-2.cockroachlabs.cloud:26257/scraperdb-11625.defaultdb?sslmode=verify-full')

    engine = create_engine('postgresql://postgres:postgresql@127.0.0.1:5432/scraper')
    
    #search for the latitude and longitude from the city
    lat, lon = get_city(searchq[1])

    insert = f"""
        WITH existing AS(
            INSERT INTO search_queries(search, city, latitude, longitude)
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
            RETURNING id
        )
        SELECT * from existing
        UNION
        SELECT id FROM search_queries where search=%s AND city=%s;
    """

    create_table = '''
            CREATE TABLE IF NOT EXISTS search_queries(
                id serial primary key,
                search TEXT,
                city TEXT,
                latitude TEXT,
                longitude TEXT,
                CONSTRAINT unique_post UNIQUE (search, city)
            );
        '''

    connection = engine.raw_connection()

    search_query_id = None
    try:
        with connection.cursor() as cursor:
            #create table if doesn't exist
            cursor.execute(create_table)
            
            
            cursor.execute(insert, 
                           (searchq[0], searchq[1], lat, lon, 
                            searchq[0], searchq[1]))
            
            #the insert query should return a id
            result = cursor.fetchone()
            
            search_query_id = result[0]
            connection.commit()
    finally:
        connection.close()

    return search_query_id
    


def insert_df_into_db(df : pd.DataFrame, searchq: tuple):
    engine = create_engine('cockroachdb://alejandro:2KmH_gJAlO3WewOGPL0Xfw@scraperdb-11625.6wr.aws-us-west-2.cockroachlabs.cloud:26257/scraperdb-11625.defaultdb?sslmode=verify-full')

    data = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(df.columns)

    searchq = [searchq]

    query = f"""
    INSERT INTO postings ({columns})
    VALUES %s ON CONFLICT (job_id) DO NOTHING;
    """

    insert = f"""
    INSERT INTO search_queries (search, city)
    VALUES %s ON CONFLICT (search, city) DO NOTHING;
    """


    additional_queries = [
        '''
            CREATE TABLE IF NOT EXISTS search_queries(
                id serial primary key,
                search TEXT,
                city TEXT,
                CONSTRAINT unique_post UNIQUE (search, city)
            );
        ''',
        '''
            CREATE TABLE IF NOT EXISTS postings(
                job_id TEXT primary key NOT NULL,
                job_title TEXT,
                company TEXT,
                date_posted TEXT,
                location TEXT,
                search_query_id INTEGER REFERENCES search_queries(id)
            );
        ''',
    ]

    connection = engine.raw_connection()

    try:
        with connection.cursor() as cursor:
            execute_values(cursor, query, data)
            execute_values(cursor, insert, searchq)
            for q in additional_queries:
                cursor.execute(q)
            
        connection.commit()
    finally:
        connection.close()



if __name__ == "__main__":
    main()
