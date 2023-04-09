import os
import re
import csv
import nltk
import yake
import logging
import pandas as pd
from dotenv import load_dotenv
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, OnSiteOrRemoteFilters

# region Setup
# Change root logger level (default is WARN)
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)

nltk.download('wordnet')
stop = stopwords.words('english')
lemmatiser = WordNetLemmatizer()
kw_extractor = yake.KeywordExtractor()
#endregion

# region Vars
title = []
company = []
date = []
link = []
description = []

load_dotenv()
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

# region Lambda Support Functions
def deEmojify(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')
def rem_en(input_txt):
    words = input_txt.lower().split()
    noise_free_words = [word for word in words if word not in stop] 
    noise_free_text = " ".join(noise_free_words) 
    return noise_free_text
def tokenize(text):
    w_tokenizer = nltk.tokenize.WhitespaceTokenizer()
    return [w for w in w_tokenizer.tokenize(text)]
def stem_eng(text):
    lemmatizer = nltk.stem.WordNetLemmatizer()
    return [lemmatizer.lemmatize(w,'v') for w in text] 
#endregion

# region Text Cleaning Functions
# Fired once on end to clean up job descriptions
def clean_description():
    features = {
        'title':title, 
        'company':company,
        'date':date,
        'link':link,
        'description':description
    }
    df = pd.DataFrame(features)
    df['description_cleaned'] = df['description']
    df["description_cleaned"] = df['description_cleaned'].replace(r'^\s*$', '#', regex=True)
    df["description_cleaned"] = df["description_cleaned"].apply(lambda s: ' '.join(re.sub("(w+://S+)", " ", str(s)).split()))
    df["description_cleaned"] = df["description_cleaned"].apply(lambda s: ' '.join(re.sub('[!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~\\-]', " ", str(s)).split()))
    df["description_cleaned"] = df["description_cleaned"].apply(lambda s: deEmojify(s))
    df["description_cleaned"] = df["description_cleaned"].apply(lambda s: rem_en(s))
    df["description_cleaned"] = df["description_cleaned"].apply(tokenize)
    df['description_cleaned'] = df['description_cleaned'].apply(stem_eng)

    # df["description_cleaned"] = df['description_cleaned'].fillna("#", inplace = True)
    # df["description_cleaned"] = df["description_cleaned"].replace('d+', '', regex=True, inplace=True)
    # df["description_cleaned"] = df["description_cleaned"].apply(lambda s: ' '.join(re.split(r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|!)\s", str(s))))
    # df["description_cleaned"] = df["description_cleaned"].apply(lambda x: tokeniser.tokenize(x))
    # df["description_cleaned"] = df["description_cleaned"].apply(lambda s: nltk.word_tokenize(s))
    # df["description_cleaned"] = df["description_cleaned"].apply(lambda tokens: [lemmatiser.lemmatize(token, pos='v') for token in tokens])

    df.to_csv('csv_file.csv', index=False)
    print('[CLEAN_DESCRIPTION]')
#endregion

# region Event Listener Functions
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
    clean_description()
    print('[ON_END]')
#endregion

# region Main 
scraper = LinkedinScraper(
    chrome_executable_path=CHROME_EXECUTABLE_PATH,  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver) 
    chrome_options=None,  # Custom Chrome options here
    headless=True,  # Overrides headless mode only if chrome_options is None
    max_workers=MAX_WORKERS,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    slow_mo=SLOW_MO,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
    page_load_timeout=PAGE_LOAD_TIMEOUT  # Page load timeout (in seconds)    
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [
    Query(
        query=JOB,
        options=QueryOptions(
            locations=LOCATIONS_LIST ,
            apply_link=True,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page mus be navigated. Default to False.
            skip_promoted_jobs=True,  # Skip promoted jobs. Default to False.
            limit=LIMIT,
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
#endregion