from datetime import datetime, timedelta
import json
import pandas as pd

from utilities.notifications import *
#
#
# def convert_json_filename_to_datetime(filename, collection_name):
#     filedate = filename.replace(collection_name, "").replace(".json", "")[:13]
#     filedate = datetime.strptime(filedate, "%Y%m%d_%H%M")
#     return filedate
#
# class JSONFile:
#     def within_interval(self, start_interval, end_interval):
#         if start_interval:
#             return (self.filedate > start_interval) and (self.filedate < end_interval)
#         else:
#             return self.filedate < end_interval
#
#     def __init__(self, filename, collection_name):
#         self.filename = filename
#         self.filedate = convert_json_filename_to_datetime(filename, collection_name)
#
#
# def upload_users_from_JSON(user_set_config, current_collection_bucket_ts, notifiers):
#
#     table_prefix = user_set_config.get_prefix()
#
#     ## Pull existing entries from the database
#     dataframe_schema = ["user_set_id", "user_id", "first_collection_bucket_ts", "last_collection_bucket_ts", "total_tweet_count", "original_tweet_count", "retweet_count", "quoted_tweet_count", "reply_count", "first_tweet_ts", "last_tweet_ts,"]
#
#     existing_user_tweet_entries = pd.DataFrame(user_set_config.db.execute_sql(user_tweet_collection_stats_fetch.format(table_prefix), fetch=True), columns=dataframe_schema)
#
#     notifications.notify_all(notifiers, "Pulled {} user_tweet_stat entries from database".format(len(user_tweet_stats)), notify_type="info")
#
#
#     ## Go through the JSON files and for each tweet add a dataframe row
#
#     notifications.notify_all(notifiers, "Crawling JSON files for new user_ids, starting at timestamp {}".format(current_collection_bucket_ts), notify_type="start")
#
#     new_user_tweet_entries = pd.DataFrame(columns=dataframe_schema) # empty dataframe to append things to
#
#
#     for json_file in json_files:
#         ## Go through the JSON files and for each tweet add a dataframe row of the form:
#         """
#         (user_set_id, user_id, current_collection_bucket_ts, current_collection_bucket_ts, 1, (1 if orig else 0), (1 if retweet else 0), (1 if quote else 0), (1 if reply else 0), tweet_ts, tweet_ts)
#         """
#         ...
#         json_file_tweet_entries = [list of rows]
#         new_user_tweet_entries = new_user_tweet_entries.append(pd.DataFrame(json_file_tweet_entries, columns=dataframe_schema), ignore_index=True)
#
#     notifications.notify_all(notifiers, "Finished crawl: {} new tweet entries ".format(current_collection_bucket_ts), notify_type="end")
#
#
#     ## Collapse by user_set_id, user_id and upload to the database
#     notifications.notify_all(notifiers, "Now combining existing entries with new entries and collapsing. ".format(current_collection_bucket_ts), notify_type="start")
#
#     user_tweet_entries = existing_user_tweet_entries.append(new_user_tweet_entries, ignore_index=True)
#
#     new_tweet_collection_stats = pd.DataFrame(user_tweet_entries.groupby(["user_set_id", "user_id"], sort=False).agg({
#         "first_collection_bucket_ts": min,
#         "last_collection_bucket_ts": max,
#         "total_tweet_count": sum,
#         "original_tweet_count": sum,
#         "retweet_count": sum,
#         "quoted_tweet_count": sum,
#         "reply_count": sum,
#         "first_tweet_ts": min,
#         "last_tweet_ts": max
#         })).reset_index()
#
#     notifications.notify_all(notifiers, "Combined and collapsed by user_set_id, user_id to {} rows".format(len(new_tweet_collection_stats), notify_type="end"))
#
#
#     ## Uploading to database
#     notifications.notify_all(notifiers, "Clearing table {}_user_tweet_collection_stats and upload new entries", notify_type="start")
#
#     db.execute_sql(SQL.user_tweet_collection_stats_clear.format(table_prefix), commit=True)
#
#     db.execute_sql(SQL.user_tweet_collection_stats_insert.format(table_prefix), args=new_tweet_collection_stats.values.tolist(), commit=True, batch_insert=True)
#
#     notifications.notify_all(notifiers, "Uploaded {} entries to table {}_user_tweet_collection_stats".format(len(new_tweet_collection_stats), table_prefix), notify_type="end")
