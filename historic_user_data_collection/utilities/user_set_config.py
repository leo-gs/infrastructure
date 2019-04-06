from datetime import datetime
import json
import random


from collection import *
from db_connect import database, SQL
from userids import *
from utilities import notifications, setup

class UserSetConfig:

    def __init__(self, db, user_set_id, notifiers):
        self.user_set_id = user_set_id
        self.db = db

        existing_data = db.execute_sql(SQL.user_set_configuration_fetch.format(user_set_id), fetch=True)

        if not bool(existing_data):
            raise ValueError("user_set_id {} doesn't exist in the database. You'll need to set parameters to create a new user_set_id or change the id to an existing entry.".format(user_set_id))

        self.db_credentials_fpath, self.twitter_credential_fpath, self.user_set_name, self.user_set_creation_ts, self.user_set_input_collection, self.user_set_input_collection_fpaths, self.user_set_description, self.user_set_notes, self.user_set_user_filter, self.user_set_user_filter_args, self.user_set_collection_modules_to_run, self.notifier_params, self.run_flags, self.day_interval, self.extra_params = existing_data[0][1:]


        ## The prefix that will be given to all user-set specific tables
        self.user_set_prefix = "c{}_{}".format(self.user_set_id,
                self.user_set_name.replace(" ", "_"))

        notifications.notify_all(notifiers, "Pulled user set configuration for `user_set_id` = {}".format(user_set_id), notify_type="info")

        self.filter_module = setup.setup_userid_module(self.user_set_prefix, self.user_set_user_filter, self.user_set_user_filter_args, notifiers)

        self.collection_modules = []
        for module in self.user_set_collection_modules_to_run:
            module_class = module["class"]
            module_args = module.get("args", {})
            self.collection_modules.append(setup.setup_collection_module(self.user_set_id, self.user_set_prefix, module_class, module_args, db, notifiers))


    def get_prefix(self):
        if self.user_set_prefix:
            return  self.user_set_prefix
        else:
            raise ValueError("This UserSet needs a user_set_id first.")


    def collect_and_upload(self, api, user_ids, notifiers):
        collection_bucket_start_ts = datetime.now()

        for module in self.collection_modules:
            chunk_size = 100
            chunk_id_padding_length = len(str(len(user_ids) // chunk_size))

            for i, chunk_start in enumerate(range(0, len(user_ids), chunk_size)):

                print("progress=" + str(int(100*(float(chunk_start)/float(len(user_ids))))))

                chunk_end = chunk_start + chunk_size
                user_id_chunk = user_ids[chunk_start : chunk_end]

                chunk_str_id = str(i).zfill(chunk_id_padding_length)

                collected_data = module.collect(api, user_id_chunk, collection_bucket_start_ts)

                module.upload_data(collected_data)
                module.dump_data(collected_data, self.user_set_name, collection_bucket_start_ts, chunk_str_id)

        collection_bucket_end_ts = datetime.now()

        for module in self.collection_modules:
            dbrow = (self.user_set_id, collection_bucket_start_ts, collection_bucket_end_ts, module.name, None)
            self.db.execute_sql(SQL.user_set_metadata_insert, args=dbrow, commit=True)

        notifications.notify_all(notifiers, "Finished collection at {}".format(str(datetime.now())), notify_type="complete")


def insert_new_user_set_config_entry_in_db(db, notifiers,         db_credentials_fpath, twitter_credential_fpath, user_set_name, user_set_input_collection, user_set_input_collection_fpaths, user_set_description, user_set_notes, user_set_user_filter, user_set_user_filter_args, user_set_collection_modules_to_run, notifier_params, run_flags, day_interval, extra_params={}):

    user_set_creation_ts = datetime.utcnow()

    dbrow = (
        db_credentials_fpath,
        twitter_credential_fpath,
        user_set_name,
        user_set_creation_ts,
        user_set_input_collection,
        json.dumps(user_set_input_collection_fpaths),
        user_set_description,
        user_set_notes,
        user_set_user_filter,
        json.dumps(user_set_user_filter_args),
        json.dumps(user_set_collection_modules_to_run),
        json.dumps(notifier_params),
        json.dumps(run_flags),
        day_interval,
        json.dumps(extra_params)
    )

    user_set_id = db.execute_sql(SQL.user_set_configuration_insert, args=dbrow, commit=True, fetch=True)[0][0]

    notifications.notify_all(notifiers, "Created new user set configuration with `user_set_id = {}`".format(user_set_id), notify_type="info")

    return user_set_id


def update_user_set_config_entry_in_db(db, notifiers, user_set_id, db_credentials_fpath, twitter_credential_fpath, user_set_name, user_set_input_collection, user_set_input_collection_fpaths, user_set_description, user_set_notes, user_set_user_filter, user_set_user_filter_args, user_set_collection_modules_to_run, notifier_params, run_flags, day_interval, extra_params={}):

    dbrow = (
        db_credentials_fpath,
        twitter_credential_fpath,
        user_set_name,
        user_set_input_collection,
        json.dumps(user_set_input_collection_fpaths),
        user_set_description,
        user_set_notes,
        user_set_user_filter,
        json.dumps(user_set_user_filter_args),
        json.dumps(user_set_collection_modules_to_run),
        json.dumps(notifier_params),
        json.dumps(run_flags),
        day_interval,
        json.dumps(extra_params),
        user_set_id
    )

    db.execute_sql(SQL.user_set_configuration_update, args=dbrow, commit=True)

    notifications.notify_all(notifiers, "Updated user set configuration with `user_set_id = {}`".format(user_set_id), notify_type="info")
