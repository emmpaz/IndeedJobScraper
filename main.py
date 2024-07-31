from dotenv import load_dotenv
from job_scraper_utils import *
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, Engine, text
import pandas as pd
import requests
from geopy.geocoders import Nominatim
from pprint import pprint
import sys
from typing import List
import threading
from queue import Queue
from functools import lru_cache
from multiprocessing import Value
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


# quick calculations
# ~1000 jobs = 176 kb = .176 mb
# so 1 million job postings = 176 gb
CLOUD = True 

def main():

    # title_list = [
    #     'product manager',
    # ]

    title_list = [
        'product manager',
        'software engineer',
        'data scientist',
        'sales engineer',
    ]
    # location_list = [
    #     'san francisco',
    #     'palo alto'
    # ]

    location_list = [
        'seattle',
        'los angeles',
        'dallas',
        'new york city',
        'portland',
        'houston',
        'philadelphia',
        'kansas city',
        'boston',
        'denver',
        'atlanta',
        'nashville',
        'san jose',
        'chicago',
        'austin',
        'wichita',
        'raleigh',
        'phoenix',
    ]

    #hashmap to get ids from search query ids
    search_query_hashmap = insert_search_query_db(title_list, location_list)
    #expensive to create an engine, so using 1 shared is more efficient. each connection is separate
    shared_engine = create_local_engine_once() if not CLOUD else create_cockroach_engine_once()
    #shared variable for threads so we need to make it race condition safe
    total_jobs = Value('i', 0)

    for location in location_list:
        thread_list = []
        resulting_queue = Queue()
        for title in title_list:
            search_query_id = search_query_hashmap[(title, location)]
            thread = threading.Thread(target=scrape_jobs_thread, args=(title, location, search_query_id, resulting_queue, total_jobs))
            thread_list.append(thread)
            thread.start()
            print(f'{'★'*5} Scraping: {title.title()} in {location.title()} {'*'*5}\n')
            #df.to_csv(r'~/Desktop/pandas.csv',sep='|',header=None, index=None, mode='a')
        
        #make sure all threads are done scraping, basically making them wait for eachother
        for thread in thread_list:
            thread.join()  

        print(f'Current total jobs found: {total_jobs.value}\n')
        while not resulting_queue.empty():
            insert_into_local(resulting_queue.get(), shared_engine)
        
        print(f'Done scraping: {location}')

    print(f'{'*'*10}\n')
    print(f'TOTAL JOBS SCRAPED: {total_jobs.value}\n')
    print(f'{'*'*10}')

def create_local_engine_once():
    return create_engine('postgresql://postgres:postgresql@127.0.0.1:5432/scraper')

def create_cockroach_engine_once():
    return create_engine('cockroachdb://alejandro:2KmH_gJAlO3WewOGPL0Xfw@scraperdb-11625.6wr.aws-us-west-2.cockroachlabs.cloud:26257/scraperdb-11625.defaultdb?sslmode=verify-full')


def scrape_jobs_thread(title, location, search_query_id, resulting_queue : Queue, total_jobs):
    driver = configure_webdriver()
    country = united_states
    date_posted = 20
    try:
        job_count = search_jobs(driver, country, title, location, date_posted, total_jobs)
        df = scrape_job_data(driver, country, total_jobs)
        df = clean_data(df)
        df['search_query_id'] = search_query_id
        resulting_queue.put(df)
    finally:
        driver.quit()


def insert_into_local(df: pd.DataFrame, engine : Engine):
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
        with connection.cursor() as cursor:
            for q in additional_queries:
                cursor.execute(q)
                    
                    #only use execute_values when we to insert a dataframe
            execute_values(connection.cursor(), query, data)
            connection.commit() 
    finally:
        connection.close()

@lru_cache(maxsize=1000)
def get_city(city : str):
    app = Nominatim(user_agent='testing script', timeout=5)

    your_loc = app.geocode(city).raw

    return [your_loc['lat'], your_loc['lon']]

def insert_search_query_db(title_list : List[tuple], location_list : List[tuple]):
    engine = create_local_engine_once() if not CLOUD else create_cockroach_engine_once()
    
    #search for the latitude and longitude from the city

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
    search_query_hash = {}
    search_query_id = None
    try:
        with connection.cursor() as cursor:
            #create table if doesn't exist
            cursor.execute(create_table)
            
            for title in title_list:
                for location in location_list:
                    lat, lon = get_city(location)
                    cursor.execute(insert, 
                                (title, location, lat, lon, 
                                    title, location))
                    #the insert query should return a id
                    result = cursor.fetchone()
                
                    search_query_hash[(title, location)] = result[0]
            connection.commit()
    finally:
        connection.close()

    return search_query_hash
    


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
