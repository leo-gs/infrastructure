# coding: utf-8

from urllib.parse import urlparse
import requests

import csv
from datetime import datetime
import json
import multiprocessing as mp
import os
import psycopg2
from psycopg2 import extras as ext
import re
import requests
import sys
import time

import sql_statements

CREATE_TABLE_STMT = "CREATE TABLE expanded_url_map(expanded_url_0 TEXT, resolved_url TEXT);"
INSERT_TWEET_STMT = "INSERT INTO expanded_url_map (expanded_url_0, resolved_url) VALUES (%s, %s);"


database_name = "disinfo_2_qanon"
db_config_file = "/home/lgs17/bowker_config.txt"


if not os.path.isdir("cache"):
    os.makedirs("cache")

#############################
## URL expansion functions ##
#############################

## Returns the domain, or None if the URL is invalid
def get_domain(url):
    p = urlparse(url)
    if p.netloc:
        return p.netloc.lower()

## Resolves the URL, but returns None if the expanded_url is invalid or couldn't be resolved.
## The `url_timeout` parameter can be used to specify how many seconds to wait for each URL.
## If domain_equivalence = True (default False), we'll stop as soon as the URL domain remains constant.
## Otherwise, keep unwinding until the entire URL is equivalent
'''
Logic:
(0) if it's a twitter.com domain, the URL has already been fully expanded, so return it
(1) expanded_url = follow_url(short_url)
(2) if expanded_url is not a valid URL, return short_url
(3) if expanded_url is a valid URL and but isn't a redirect or expanded_url == short_url
        we assume short_url has already been completely expanded and return short_url
(4) otherwise set short_url <- expanded_url and repeat from beginning
'''
def expand_url(short_url, url_timeout=None, domain_equivalence=False):
    short_domain = get_domain(short_url)
    if short_domain == "twitter.com":
        return (short_url, short_domain)

    expanded_url = None
    # try:

    response = requests.head(short_url, timeout=url_timeout)
    if response.is_redirect:
        expanded_url = response.headers["location"]
    else:
        expanded_url = short_url
    # except:
    #     print(short_url)

    ## We weren't able to expand the URL, so the URL is broken
    if not expanded_url:
        return (None, short_domain)

    expanded_domain = get_domain(expanded_url)

    ## The expanded_url isn't valid
    if not expanded_domain:
        return (None, short_domain)

    ## Expanding the URL took us to the same domain, so it was already completely expanded
    if domain_equivalence:
        if short_domain == expanded_domain:
            return (short_url, short_domain)
    else:
        if short_url == expanded_url:
            return (short_url, short_domain)
    
    ## Otherwise, following the URL took us to a new URL or domain so start again with the new URL to see if there's more expansion to do
    return expand_url(expanded_url)


########################
## Database functions ##
########################

def open_connection():
    config = {"database": database_name}
    for line in open(db_config_file).readlines():
        key, value = line.strip().split("=")
        config[key] = value

    database = psycopg2.connect(**config)
    cursor = database.cursor()
    return database, cursor

def close_connection(database, cursor):
    cursor.close()
    database.commit()
    database.close()


###############################################
## 1. Load the distinct short urls to expand ##
###############################################

short_urls = []

## If there's no cached file, pull them from the database
## Note: it's important that the URLs remain in the same order, which is why they're also sorted
if not os.path.isfile("cache/distinct_urls_" + database_name + ".json"):
    database, cursor = open_connection()
    cursor.execute("SELECT DISTINCT expanded_url_0 FROM tweets WHERE expanded_url_0 IS NOT NULL")
    short_urls = sorted([u[0] for u in cursor.fetchall()])

    with open("cache/distinct_urls_" + database_name + ".json", "w+") as f:
        json.dump(short_urls, f)
    close_connection(database, cursor)

## Otherwise we have a cached file of URLs already, so we can just use that
else:
    with open("cache/distinct_urls_" + database_name + ".json", "w+") as f:
        short_urls = json.load(f)


##########################################################################
## 2. Split the URLs into chunks that can be passed to parallel workers ##
##########################################################################

chunk_size = 1000 ## how many URLs per chunk - can be adjusted
url_chunks = []

index = 0
while index < len(short_urls):
    chunk = (index, short_urls[index : (index + chunk_size)])
    url_chunks.append(chunk)
    index = index + chunk_size


###################################################################################
## 3. Start parallel jobs to process each chunk with as many cores as we can get ##
###################################################################################

def process_chunk(chunk):
    chunk_index, urls_to_expand = chunk
    print("Processing chunk {}".format(chunk_index), flush=True)

    ## Only expand the URLs if we haven't already expanded and cached them
    if not os.path.isfile("cache/{}.json".format(chunk_index)):
        expanded_urls = [expand_url(short_url, url_timeout=60) for short_url in urls_to_expand]

        ## Cache the chunk so if something goes wrong later we don't have to expand everything again
        with open("cache/{}.json".format(chunk_index)) as f:
            json.dump(expanded_urls, f)

pool = mp.Pool()
_ = pool.map_async(proces_chunk, url_chunks)
pool.close()
pool.join()


## Compile all URL chunks into one big file

## Then load into database

print("Done")

