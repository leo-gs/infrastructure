# coding: utf-8

from urllib.parse import urlparse
import requests
from urllib.request import urlopen

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

UPDATE_STMT = "UPDATE expanded_url_map SET resolved_url = %s, domain = %s WHERE expanded_url_0 = %s;"


database_name = "database_name"
db_config_file = "database_config.txt"


if not os.path.isdir("cache_4"):
    os.makedirs("cache_4")

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

Returns: (original_url, expanded_url (or None), expanded_domain (or None))
'''
def expand_url(orig_url, short_url, url_timeout=60, steps_remaining=25, domain_equivalence=False):
    print("{} -> {}".format(orig_url, short_url), flush=True) 
    short_domain = get_domain(short_url)
    if short_domain == "twitter.com":
        return (short_url, short_domain, orig_url)

    expanded_url = None
    
    try:
        response = requests.get(short_url, timeout=url_timeout)
        if response.status_code == 200:
            expanded_url = response.url
        else:
            print("{}: {}".format(short_url, response.status_code), flush=True)
    except:
        print("Connection Error: {}".format(short_url), flush=True)

    ## We weren't able to expand the URL, so the URL is broken
    if not expanded_url:
        return (None, None, orig_url)

    expanded_domain = get_domain(expanded_url)

    ## The expanded_url isn't valid
    if not expanded_domain:
        return (None, None, orig_url)

    ## Expanding the URL took us to the same place, so it was already completely expanded
    if domain_equivalence:
        if short_domain == expanded_domain:
            return (short_url, short_domain, orig_url)
    else:
        if short_url == expanded_url:
            return (short_url, short_domain, orig_url)
    
    ## Otherwise, following the URL took us to a new URL or domain so start again with the new URL to see if there's more expansion to do
    if steps_remaining > 0:
        return expand_url(orig_url, expanded_url, url_timeout=url_timeout, steps_remaining=(steps_remaining-1), domain_equivalence=domain_equivalence)
    else:
        return (short_url, short_domain, orig_url)

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

# If there's no cached file, pull them from the database
# Note: it's important that the URLs remain in the same order, which is why they're also sorted
if not os.path.isfile("cache_4/distinct_urls_" + database_name + ".json"):
    database, cursor = open_connection()
    cursor.execute("SELECT expanded_url_0 FROM expanded_url_map WHERE domain IS NULL")
    short_urls = sorted([u[0] for u in cursor.fetchall()])
    close_connection(database, cursor)

    with open("cache_4/distinct_urls_" + database_name + ".json", "w+") as f:
        json.dump(short_urls, f)

## Otherwise we have a cached file of URLs already, so we can just use that
else:
    with open("cache_4/distinct_urls_" + database_name + ".json") as f:
        short_urls = json.load(f)

print("{} URLs pulled".format(len(short_urls)), flush=True)


##########################################################################
## 2. Split the URLs into chunks that can be passed to parallel workers ##
##########################################################################

chunk_size = 100 ## how many URLs per chunk - can be adjusted
url_chunks = []

index = 0
while index < len(short_urls):
    chunk_id = index // chunk_size
    chunk = (chunk_id, short_urls[index : (index + chunk_size)])
    url_chunks.append(chunk)
    index = index + chunk_size


###################################################################################
## 3. Start parallel jobs to process each chunk with as many cores as we can get ##
###################################################################################

def process_chunk(chunk):
    chunk_id, urls_to_expand = chunk
    print("{}/{}".format(chunk_id, len(url_chunks)), flush=True)

    ## Only expand the URLs if we haven't already expanded and cached them
    if not os.path.isfile("cache_4/{}.json".format(chunk_id)):
        print("Processing chunk {}".format(chunk_id), flush=True)
        expanded_urls = [expand_url(short_url, short_url, url_timeout=60, steps_remaining=25) for short_url in urls_to_expand]

        ## Cache the chunk so if something goes wrong later we don't have to expand everything again
        with open("cache_4/{}.json".format(chunk_id), "w+") as f:
            json.dump(expanded_urls, f)

    else:
        print("Chunk {} already expanded".format(chunk_id), flush=True)


pool = mp.Pool()
_ = pool.map_async(process_chunk, url_chunks)
pool.close()
pool.join()

#for url_chunk in url_chunks:
#    process_chunk(url_chunk)

print("Finished processing URLs", flush=True)


###############################
## 4. Compile all URL chunks ##
###############################

# Make sure we've got them all
for chunk_id, _ in url_chunks:
    assert os.path.isfile("cache_4/{}.json".format(chunk_id)), "No file exists for chunk {}".format(chunk_id)

## Compile all URL chunks into one big file
compiled_urls = []
for chunk_id, _ in url_chunks:
    fname = "cache_4/{}.json".format(chunk_id)
    print(fname, flush=True)
    with open(fname) as f:
        chunk_urls = json.load(f)
        compiled_urls.extend(chunk_urls)
compiled_urls = [c for c in compiled_urls if c[1]] # No need to update if resolving the URL wasn't successful
print("Finished compiling {} URLs".format(len(compiled_urls)), flush=True)


###############################
## 5. Load into the database ##
###############################
database, cursor = open_connection()
ext.execute_batch(cursor, UPDATE_STMT, compiled_urls)
close_connection(database, cursor)

print("Updated database", flush=True)

##############
## 6. Done! ##
##############

print("Done!")

