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
    job_location = 'San francisco'
    date_posted = 10

    sorted_df = None

    try:
        full_url = search_jobs(driver, country, job_position, job_location, date_posted)
        df = scrape_job_data(driver, country)
        df = clean_data(df)
        df.to_csv(r'~/Desktop/pandas.csv',sep='|',header=None, index=None, mode='a')
        insert_df_into_db(df)
        # connection = get_connection()
        # connection.autocommit = True
        # cur = connection.cursor()
        # sql_create = '''
        #                 CREATE TABLE IF NOT EXISTS postings(
        #                     job_id TEXT primary key NOT NULL,
        #                     job_title TEXT,
        #                     company TEXT,
        #                     date_posted TEXT,
        #                     location TEXT
        #                 );
        #                 '''
        # cur.execute(sql_create)
        # sql = '''COPY postings(job_id, job_title, company, date_posted, location) FROM '/Users/manny/Desktop/pandas.csv' DELIMITER '|' CSV HEADER;'''
        # cur.execute(sql)
        if df.shape[0] == 1:
            print("No results found. Something went wrong.")
            subject = 'No Jobs Found on Indeed'
            body = """
            No jobs were found for the given search criteria.
            Please consider the following:
            1. Try adjusting your search criteria.
            2. If you used English search keywords for non-English speaking countries,
               it might return an empty result. Consider using keywords in the country's language.
            3. Try more general keyword(s), check your spelling or replace abbreviations with the entire word

            Feel free to try a manual search with this link and see for yourself:
            Link {}
            """.format(full_url)

        else:
            print('f')
            # cleaned_df = clean_data(df)
            # sorted_df = sort_data(cleaned_df)
    finally:
        try:
            if sorted_df is not None:
                print('f')
        except Exception as e:
            print(f"Error sending email: {e}")
        finally:
            pass
            driver.quit()

def get_connection():
    try:
        return psycopg2.connect(
            database="scraper",
            user="postgres",
            password="postgres",
            host="127.0.0.1",
            port='5432'
        )
    except:
        return False


def insert_df_into_db(df : pd.DataFrame):
    engine = create_engine('postgresql://alejandro:2KmH_gJAlO3WewOGPL0Xfw@scraperdb-11625.6wr.aws-us-west-2.cockroachlabs.cloud:26257/scraperdb.defaultdb?sslmode=verify-full')

    data = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(df.columns)

    query = f"""
    INSERT INTO postings ({columns})
    VALUES %s ON CONFLICT (job_id) DO NOTHING
    """

    connection = engine.raw_connection()

    try:
        with connection.cursor() as cursor:
            execute_values(cursor, query, data)
        connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
