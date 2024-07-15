import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta

global total_jobs


def configure_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )

    return driver


def search_jobs(driver, country, job_position, job_location, date_posted):
    full_url = f'{country}/jobs?q={"+".join(job_position.split())}&l={job_location}&fromage={date_posted}'
    print(full_url)
    driver.get(full_url)
    global total_jobs
    try:
        job_count_element = driver.find_element(By.XPATH,
                                                '//div[starts-with(@class, "jobsearch-JobCountAndSortPane-jobCount")]')
        total_jobs = job_count_element.find_element(By.XPATH, './span').text
        print(f"{total_jobs} found")
    except NoSuchElementException:
        print("No job count found")
        total_jobs = "Unknown"

    return full_url


def scrape_job_data(driver, country):
    df = pd.DataFrame({'job_id': [] ,'job_title': [], 'company': [],
                       'date_posted': [], 'location': []})
    job_count = 0
    # count = 0
    while True and job_count < 10:
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

            try:
                date_posted = i.find('span', class_='date').text
            except AttributeError:
                date_posted = i.find('span', {'data-testid': 'myJobsStateDate'}).text.strip()
                date_posted = date_posted.replace('Posted', '', 1)

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
                    print(job_id)
            
            if job_title != '':
                new_data = pd.DataFrame({ 'job_id' : [job_id],'job_title': [job_title],
                                     'company': [company],
                                     'date_posted': [date_posted],
                                     'location': [location]});

            df = pd.concat([df, new_data], ignore_index=True)
            job_count += 1

        print(f"Scraped {job_count} of {total_jobs}")

        try:
            next_page = soup.find('a', {'aria-label': 'Next Page'}).get('href')

            next_page = country + next_page
            driver.get(next_page)

        except:
            break

    return df


def convert_indeed_date(date_string):
    today = datetime.now().date()

    for item in ['Just posted','hours ago', 'Today', 'hour ago']:
        if item in date_string:
            return today
    if 'days ago' in date_string or 'day ago' in date_string:
        print(date_string.split()[1])
        days = int(date_string.split()[1])
        return today - timedelta(days=days)
    elif 'months ago' or 'month ago' in date_string:
        print(date_string.split()[1])
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


def send_email(df, sender_email, receiver_email, job_position, job_location, password):
    sender = sender_email
    receiver = receiver_email
    password = password
    msg = MIMEMultipart()
    msg['Subject'] = 'New Jobs from Indeed'
    msg['From'] = sender
    msg['To'] = ','.join(receiver)

    attachment_filename = generate_attachment_filename(job_position, job_location)

    csv_content = df.to_csv(index=False).encode()

    part = MIMEBase('application', 'octet-stream')
    part.set_payload(csv_content)
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename="{attachment_filename}"')
    msg.attach(part)

    s = smtplib.SMTP_SSL(host='smtp.gmail.com', port=465)
    s.login(user=sender, password=password)

    s.sendmail(sender, receiver, msg.as_string())

    s.quit()


def send_email_empty(sender, receiver_email, subject, body, password):
    msg = MIMEMultipart()
    password = password

    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ','.join(receiver_email)

    # Attach the body as the text/plain part of the email
    msg.attach(MIMEText(body, 'plain'))

    s = smtplib.SMTP_SSL(host='smtp.gmail.com', port=465)
    s.login(user=sender, password=password)

    s.sendmail(sender, receiver_email, msg.as_string())

    s.quit()


def generate_attachment_filename(job_title, job_location):
    filename = f"{job_title.replace(' ', '_')}_{job_location.replace(' ', '_')}.csv"
    return filename
