# region Imports
import os
import re
import csv
import boto3
import logging
import pandas as pd
from selenium import webdriver
# from dotenv import load_dotenv
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, OnSiteOrRemoteFilters
#endregion


# region Setup
# Change root logger level (default is WARN)
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
#endregion


# region Vars
title = []
company = []
date = []
link = []
description = []

# load_dotenv()
CHROME_EXECUTABLE_PATH = str(os.getenv('CHROME_EXECUTABLE_PATH'))
BINARY_LOCATION = str(os.getenv('BINARY_LOCATION'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS'))
SLOW_MO = int(os.getenv('SLOW_MO'))
PAGE_LOAD_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT'))
JOB = os.getenv('JOB')
LOCATIONS_LIST = [item for item in os.getenv('LOCATIONS_LIST').split(",") if item]
LIMIT = int(os.getenv('LIMIT'))
print(f'MAX_WORKERS: {MAX_WORKERS} | SLOW_MO: {SLOW_MO} | PAGE_LOAD_TIMEOUT: {PAGE_LOAD_TIMEOUT} | JOB: {JOB} | LOCATIONS_LIST: {LOCATIONS_LIST} | LIMIT: {LIMIT}')
#endregion


# region Functions
# Fired once for each successfully processed job
def on_data(data: EventData):
    title.append(data.title.strip())
    company.append(data.company.strip())
    date.append(data.date.strip())
    link.append(data.link.strip())
    description.append(data.description.strip())
    print('[ON_DATA]', data.title, data.company, data.date, data.link, len(data.description))
# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))
# Fired on error
def on_error(error):
    print('[ON_ERROR]', error)
# Fired once on end
def on_end():
    save_csv_to_s3()
    print('[ON_END]')


# Fired once on end to clean up job descriptions
def save_csv_to_s3():
    features = {
        'title':title, 
        'company':company,
        'date':date,
        'link':link,
        'description':description
    }
    df = pd.DataFrame(features)
    
    # Properly call your s3 bucket
    s3_client = boto3.client('s3')
    df.to_csv('/tmp/csv_file.csv', index=False)
    response = s3_client.upload_file('/tmp/csv_file.csv', 'nik-jobs-data', 'linkedin_jobs.csv')
    print('[CLEAN_DESCRIPTION_and_save_csv_to_s3]')
#endregion


def handler(event, context): 
    try:
        # region Main 
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280x1696")
        options.add_argument("--single-process")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--no-zygote")
        options.add_argument("--remote-debugging-port=9222")
        options.binary_location = '/opt/chrome/chrome'

        scraper = LinkedinScraper(
            chrome_executable_path='/opt/chromedriver',  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver) 
            chrome_options=options,  # Custom Chrome options here
            headless=True,  # Overrides headless mode only if chrome_options is None
            max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
            slow_mo=2,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
            page_load_timeout=40  # Page load timeout (in seconds)    
        )

        # Add event listeners
        scraper.on(Events.DATA, on_data)
        scraper.on(Events.ERROR, on_error)
        scraper.on(Events.END, on_end)

        queries = [
            Query(
                query='Data Scientist',
                options=QueryOptions(
                    locations=['United States','Texas'] ,
                    apply_link=True,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page mus be navigated. Default to False.
                    skip_promoted_jobs=True,  # Skip promoted jobs. Default to False.
                    limit=3,
                    filters=QueryFilters(
                        # company_jobs_url='https://www.linkedin.com/jobs/search/?f_C=1441%2C17876832%2C791962%2C2374003%2C18950635%2C16140%2C10440912&geoId=92000000',  # Filter by companies.                
                        relevance=RelevanceFilters.RELEVANT,
                        time=TimeFilters.MONTH,
                        type=[TypeFilters.FULL_TIME],
                        on_site_or_remote=[OnSiteOrRemoteFilters.ON_SITE, OnSiteOrRemoteFilters.REMOTE, OnSiteOrRemoteFilters.HYBRID],
                        experience=[ExperienceLevelFilters.ENTRY_LEVEL, ExperienceLevelFilters.ASSOCIATE, ExperienceLevelFilters.MID_SENIOR]
                    )
                )
            ),
        ]

        scraper.run(queries)
        return {
            'statusCode': 200,
            'body': 'Completed'
        }
    except Exception as e: 
        logger.error(f'\nError: {e}\n')
        response = boto3.client('sns').publish(
            TopicArn = 'arn:aws:sns:ap-south-1:832214191436:ScrapeLinkedInJobsMail',
            Message = str(e),
            Subject='LinkedIn Job Scraper Failed'
        )
        return {
            'statusCode': 400,
            'body': 'Failed'
        }