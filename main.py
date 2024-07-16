from dotenv import load_dotenv
from job_scraper_utils import *
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import pandas as pd
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
    job_position = 'software engineer'
    job_location = 'new york city'
    date_posted = 10

    sorted_df = None

    try:
        full_url = search_jobs(driver, country, job_position, job_location, date_posted)
        df = scrape_job_data(driver, country)
        df = clean_data(df)
        searchq = (job_position, job_location)
        df.to_csv(r'~/Desktop/pandas.csv',sep='|',header=None, index=None, mode='a')
        insert_df_into_db(df, searchq)
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
    

# def insert_into_local(df: pd.DataFrame):
#     engine = create_engine('postgresql://postgres:postgresql@127.0.0.1:5432/scraper')

#     data = [tuple(x) for x in df.to_numpy()]
#     columns = ','.join(df.columns)

#     query = f"""
#     INSERT INTO postings ({columns})
#     VALUES %s ON CONFLICT (job_id) DO NOTHING;
#     """

#     additional_queries = [
#         '''
#             CREATE TABLE IF NOT EXISTS search_queries(
#                 id serial primary key,
#                 search TEXT,
#                 city TEXT
#             );
#         ''',
#         '''
#             CREATE TABLE IF NOT EXISTS postings(
#                 job_id TEXT primary key NOT NULL,
#                 job_title TEXT,
#                 company TEXT,
#                 date_posted TEXT,
#                 location TEXT,
#                 search_query_id INTEGER REFERENCES search_queries(id)
#             );
#         ''',
#     ]

#     connection = engine.raw_connection()

#     try:
#         for q in additional_queries:
#             connection.cursor().execute(q)
#         execute_values(connection.cursor(), query, data)
#         connection.commit()
#     finally:
#         connection.close()


def insert_df_into_db(df : pd.DataFrame, searchq: tuple):
    engine = create_engine('cockroachdb://alejandro:2KmH_gJAlO3WewOGPL0Xfw@scraperdb-11625.6wr.aws-us-west-2.cockroachlabs.cloud:26257/scraperdb-11625.defaultdb?sslmode=verify-full')

    data = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(df.columns)

    searchq = [searchq]
    print(data)
    print(searchq)

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
# def insert_into_search_queries(search_query: str, city: str):
#     engine = create_engine('cockroachdb://alejandro:2KmH_gJAlO3WewOGPL0Xfw@scraperdb-11625.6wr.aws-us-west-2.cockroachlabs.cloud:26257/scraperdb-11625.defaultdb?sslmode=verify-full')
    
#     query = "SELECT * FROM postings;"
    
#     connection = engine.raw_connection()

#     try:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             results = cursor.fetchall()
#             for job in results:
#                 print(job)
#                 if job[5] not in search_queries:
#                     cursor.execute("INSERT INTO search_queries (search, city) VALUES (%s, %s);", (search_query, city))
                
            

            
#         connection.commit()
#     finally:
#         connection.close()


if __name__ == "__main__":
    main()
