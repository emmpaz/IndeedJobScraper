import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException
import selenium.webdriver
from selenium.webdriver.common.by import By
import selenium.webdriver.remote
import selenium.webdriver.remote.webdriver
from selenium_stealth import stealth
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import selenium
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
from webdriver_manager.core.os_manager import ChromeType
from multiprocessing import Value


def configure_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-gpu")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-blink-features=AutomationControlled")

    chromedriver_path = '/Users/manny/chrome/chromedriver'

    driver = webdriver.Chrome(service=ChromeService(executable_path=chromedriver_path), options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Mac Intel",
            webgl_vendor="Apple Inc.",
            renderer="Apple GPU",
            fix_hairline=True,
            )

    return driver


def linkedin(driver : selenium.webdriver.remote.webdriver.WebDriver):
    linkedin_url = 'https://www.linkedin.com/jobs/search/?currentJobId=3901936295&f_TPR=r86400&keywords=software%20engineer&location=new%20york%20city&distance=0&f_TPR=r86400&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true'

    driver.get(linkedin_url)

    try:
        driver.save_screenshot('screenshot.png')
        job_count_element = driver.find_element(By.CLASS_NAME, 'results-context-header__context')
        total_jobs = job_count_element.find_element(By.CLASS_NAME, 'results-context-header__job-count').text
        print(f"{total_jobs} found")
    except:
        print("No job count found")
        return

    def get_job_size():
        try:
            job_list = driver.find_element(By.CSS_SELECTOR, 'ul.jobs-search__results-list')
            return len(job_list.find_elements(By.TAG_NAME, 'li'))
        except NoSuchElementException:
            print('none')
            return 0

    def scroll_down():
        last_height = driver.execute_script('return document.body.scrollHeight')
        try:
            initial_size = get_job_size()
            print(f'Initial job list size: {initial_size}')
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            # button = driver.find_element(By.CLASS_NAME, 'infinite-scroller__show-more-button')
            # driver.execute_script('arguments[0].scrollIntoView(true);', button)
            time.sleep(4)
            new_height = driver.execute_script('return document.body.scrollHeight;')
            print(new_height)
            #driver.execute_script('arguments[0].click()', button);
            new_size = get_job_size()
            print(f'new job list size: {new_size}')
            if new_height == last_height:
                return False
            
            last_height = new_height

            return True
        except (TimeoutException, NoSuchElementException, ElementClickInterceptedException) as e:
            print(f'Failed: {str(e)}')
            return False

    while True:
        if scroll_down():
            print(' ')
        else:
            print('no found')
            break
    return


def search_jobs(driver, country, job_position, job_location, date_posted, total_jobs):
    full_url = f'{country}/jobs?q={"+".join(job_position.split())}&l={job_location}&radius=0&fromage={date_posted}'
    driver.get(full_url)
    try:
        job_count_element = driver.find_element(By.XPATH,
                                                '//div[starts-with(@class, "jobsearch-JobCountAndSortPane-jobCount")]')
        job_count = job_count_element.find_element(By.XPATH, './span').text
    except NoSuchElementException:
        print("No job count found")


    return None


def scrape_job_data(driver, country, total_jobs):
    df = pd.DataFrame({'job_id': [] ,'job_title': [], 'company': [],
                       'date_posted': [], 'location': []})
    job_count = 0
    # count = 0
    while True:
        # count += 1
        soup = BeautifulSoup(driver.page_source, 'lxml')

        boxes = soup.find_all('div', class_='job_seen_beacon')

        for i in boxes:
            link = i.find('a').get('href')
            link_full = country + link
            job_title = i.find('a', class_='jcs-JobTitle css-jspxzf eu4oa1w0').text
            # Check if the 'Company' attribute exists
            company_tag = i.find('span', {'data-testid': 'company-name'})
            company = company_tag.text if company_tag else None
            MAX_TRIES = 3
            tries = 1
            date_posted = None
            date_spans = None
            try:
                date_spans = i.find('span', class_='date').text
                if date_spans:
                    date_posted = date_spans.get_text(strip=True, separator=' ')
                    date_posted = date_posted.replace('Posted', '', 1)
                    date_posted = date_posted.replace('Employer', '', 1)
            except AttributeError:
                date_spans = i.find('span', {'data-testid': 'myJobsStateDate'})
                if date_spans:
                    date_posted = date_spans.get_text(strip=True, separator=' ')
                    date_posted = date_posted.replace('Posted', '', 1)
                    date_posted = date_posted.replace('Employer', '', 1)

            location_element = i.find('div', {'data-testid': 'text-location'})
            location = ''
            if location_element:
                # Check if the element contains a span
                span_element = location_element.find('span')

                if span_element:
                    location = span_element.text
                else:
                    location = location_element.text
                location = location.replace('&nbsp;', ' ')
            
            job_id = ''
            if job_title:
                span_element = i.find('a', class_='jcs-JobTitle css-jspxzf eu4oa1w0').span
                if span_element:
                    job_id = span_element.get('id')
            
            if job_title != '':
                new_data = pd.DataFrame({ 'job_id' : [job_id],'job_title': [job_title],
                                     'company': [company],
                                     'date_posted': [date_posted],
                                     'location': [location]});

            df = pd.concat([df, new_data], ignore_index=True)
            with total_jobs.get_lock():
                total_jobs.value += 1

        try:
            next_page = soup.find('a', {'aria-label': 'Next Page'}).get('href')

            next_page = country + next_page
            driver.get(next_page)

        except:
            break

    return df


def convert_indeed_date(date_string):
    today = datetime.now().date()

    if not date_string:
        return None

    for item in ['Just posted','hours ago', 'Today', 'hour ago']:
        if item in date_string:
            return today
    if 'Hiring ongoing' in date_string:
        return today
    elif 'days ago' in date_string or 'day ago' in date_string:
        days = int(date_string.split()[1])
        return today - timedelta(days=days)
    elif 'months ago' or 'month ago' in date_string:
        months = int(date_string.split()[1])
        return today - timedelta(days=months*30)
    elif '30+ days ago' in date_string:
        return today - timedelta(days=30)
    else:
        return 'NA'

def clean_data(df):
    df['date_posted'] = df['date_posted'].apply(convert_indeed_date)
    return df


def save_csv(df, job_position, job_location):
    def get_user_desktop_path():
        home_dir = os.path.expanduser("~")
        desktop_path = os.path.join(home_dir, "Desktop")
        return desktop_path

    file_path = os.path.join(get_user_desktop_path(), '{}_{}'.format(job_position, job_location))
    csv_file = '{}.csv'.format(file_path)
    df.to_csv('{}.csv'.format(file_path), index=False)

    return csv_file


