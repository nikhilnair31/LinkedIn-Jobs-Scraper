
import yake
import pandas as pd

language = "en"
max_ngram_size = 2
deduplication_threshold = 0.1
numOfKeywords = 20
custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)

def print_keywords(df):
    text = ' '.join(df['description_cleaned'])
    keywords = custom_kw_extractor.extract_keywords(text)
    for kw in keywords:
        print(kw)

def read_csv_print_keywords():
    df = pd.read_csv('csv_file.csv')
    text = ' '.join(df['description_cleaned'])
    keywords = custom_kw_extractor.extract_keywords(text)
    for kw in keywords:
        print(kw)

read_csv_print_keywords()