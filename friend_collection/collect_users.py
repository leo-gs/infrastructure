
# coding: utf-8

# In[1]:


import csv
from datetime import datetime, timedelta
import json
import os
import pandas as pd


# # Input parameters
# `JSONFile` keeps track of both the filename and its date/time of creation (since there might be multiple files per hour).
# `start_interval` and `end_interval` are exclusive time bounds on running which JSON files to process.
# 
# This section first gets a list of all files in the `data_dir` and filters to only those with JSON file extensions.
# Then it converts all of those to `JSONFile` objects.
# To filter by time window, it checks whether the `progress_file` exists, and if so, finds the name of the last JSON file processed and uses that to set `start_interval` (otherwise there's no `start_interval`). `end_interval` is set to be the current hour rounded down. I made this decision because the logic is more simple if I process each hour's files at once, so it's easier to wait until the hour is over before processing its files. **TODO: check in the capture code to see when the JSON files are actually dumped.**

# In[6]:


def convert_json_filename_to_datetime(filename, collection_name):
    filedate = filename.replace(collection_name, "").replace(".json", "")[:13]
    filedate = datetime.strptime(filedate, "%Y%m%d_%H%M")
    return filedate

class JSONFile:

    def within_interval(self, start_interval, end_interval):
        if start_interval:
            return (self.filedate > start_interval) and (self.filedate < end_interval)
        else:
            return self.filedate < end_interval
    
    def __init__(self, filename, collection_name):
        self.filename = filename
        self.filedate = convert_json_filename_to_datetime(filename, collection_name)

def get_time_window(progress_file, collection_name):
    start_interval = None
    if os.path.isfile(progress_file):
        progress_filename = json.load(open(progress_file))["last_file_processed"]
        start_interval = convert_json_filename_to_datetime(progress_filename, collection_name)
    
    end_interval = datetime.now().replace(minute=0, second=0, microsecond=0) ## Set the interval end to be at the hour
    
    return start_interval, end_interval

def update_progress_file(collection_files, progress_file):
    if collection_files:
        print("dumping")
        last_file = max(collection_files, key=lambda jfile: jfile.filedate)
        progress = {"last_file_processed": last_file.filename}
        json.dump(progress, open(progress_file, "w+"))


# # Pull user_ids from a JSON file

# In[3]:


def get_user_id_list_from_file(filepath):
	user_ids = []
	with open(filepath) as infile:
		for line in infile:
			if (not line or len(line) < 2):
				continue
			try:
				tweet = json.loads(line)
				user_id = tweet["user"]["id"]
				user_ids.append(user_id)
			except ValueError:
				print("bad json: " + line)
				continue
	return user_ids


# # Run everything

# In[4]:


def run(configuration):
    collection_name = configuration["collection_name"]
    
    ## Get all the JSON files
    collection_files = os.listdir(configuration["data_dir"])
    collection_files = list(filter(lambda f: f[-5:] == ".json", collection_files)) ## Filter out .tmp files
    collection_files = [JSONFile(f, collection_name) for f in collection_files]
    
    ## Filter out the already-processed JSON files by time window
    progress_file = configuration["progress_file"]
    start_interval, end_interval = get_time_window(progress_file, collection_name)
    collection_files = list(filter(lambda jfile: jfile.within_interval(start_interval, end_interval), collection_files)) ## Filter to time window
    
    ## Update the file that stores the time window
    update_progress_file(collection_files, progress_file)
    
    ## Get all the user_ids from the new JSON files
    collections_dir = configuration["data_dir"]
    user_ids = []
    for jfile in collection_files:
        user_ids.extend(get_user_id_list_from_file(os.path.join(collections_dir, jfile.filename)))

    ## Get user_ids already written to the file (if the file exists)
    user_ids_file = configuration["user_ids_csv"]
    if os.path.isfile(user_ids_file):
        current_user_ids = pd.read_csv(user_ids_file, header=None, names=["user_id"])["user_id"].tolist()
        user_ids = user_ids + current_user_ids
    
    ## Drop any duplicates
    user_ids = set(user_ids)

    ## Rewrite user_id file
    with open(user_ids_file, "w+") as f:
        writer = csv.writer(f)
        for user_id in user_ids:
            writer.writerow([user_id])
    
    return (user_ids, start_interval, end_interval)

