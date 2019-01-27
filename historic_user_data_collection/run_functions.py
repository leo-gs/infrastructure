from crontab import CronTab
import json
import os
import psycopg2
from random import shuffle
import sys

from credentials import credentials
from db_connect import database, SQL
from utilities import notifications, user_set_config, setup


def initialize_collection(config_fpath):

    """ Setting things up """

    with open(config_fpath) as f:
        config = json.load(f)

    db_credentials_fpath = config["db_credentials_fpath"]

    ## Name of database (should already exist)
    db_name = config["database_name"]
    del config["database_name"]

    # Get whatever notifiers are requested in the config file
    # notifiers = get_notifiers(config)
    notifiers = [notifications.TestNotification()]

    # Prepare server (Step 0)
    db_creds = credentials.read_credential_file(db_credentials_fpath)
    host, user, password = db_creds["host"], db_creds["user"], db_creds["password"]
    db = setup.server_setup(host, user, password, db_name, notifiers)

    # Creating entry in database
    user_set_id = user_set_config.insert_new_user_set_config_entry_in_db(
        db, notifiers, **config)

    # Write updated configuration file
    config["database_name"] = db_name
    config["user_set_id"] = user_set_id
    with open(config_fpath, "w+") as f:
        json.dump(config, f, sort_keys=True, indent=4)


def run_collection(config_fpath):

    with open(config_fpath) as f:
        config = json.load(f)

    """ Setting things up """

    db_credentials_fpath = config["db_credentials_fpath"]
    twitter_credential_fpath = config["twitter_credential_fpath"]

    # ID of historic collection in database
    user_set_id = config["user_set_id"]

    # Database
    db_name = config["database_name"]
    del config["database_name"]

    # Get whatever notifiers are requested in the config file
    # notifiers = get_notifiers(config)
    notifiers = [notifications.TestNotification()]

    # Prepare server (Step 0)
    db_creds = credentials.read_credential_file(db_credentials_fpath)
    host, user, password = db_creds["host"], db_creds["user"], db_creds["password"]

    db = setup.server_setup(host, user, password, db_name, notifiers)

    user_set_config.update_user_set_config_entry_in_db(
        db, notifiers, **config)

    usc = user_set_config.UserSetConfig(db, user_set_id, notifiers)

    # Last thing before collecting data: get Twitter authentication
    twitter_creds = credentials.read_credential_file(twitter_credential_fpath)
    twitter_api = credentials.get_twitter_api_obj(**twitter_creds)

    """ Running the collection """

    ## Pulling user ids to collect on
    user_ids = usc.filter_module.pull_users()

    ## Shuffle the list so collection is run on a different order each time.
    shuffle(user_ids)

    usc.collect_and_upload(twitter_api, user_ids, notifiers)


def schedule_cron_job(config_fpath, every_x_days=10):
    working_dir = os.path.abspath(".")

    cron_command = '''python3 {}/run_functions.py "{}"'''.format(working_dir, config_fpath)

    cron = CronTab(user=True)
    cronjob = cron.new(command=cron_command)

    cronjob.day.every(every_x_days)

    cron.write()

    print("Running every {} days: `{}`".format(every_x_days, cron_command), flush=True)


if __name__ == "__main__":
    config_fpath = sys.argv[1]
    run_collection(config_fpath)
