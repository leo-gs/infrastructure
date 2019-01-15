user_set_configuration_create = """
    CREATE TABLE IF NOT EXISTS user_set_configuration (
        user_set_id SERIAL PRIMARY KEY UNIQUE,
        user_set_name TEXT,
        user_set_creation_ts TIMESTAMP,
        user_set_input_collection TEXT,
        user_set_input_collection_fpaths JSON,
        user_set_description TEXT,
        user_set_notes TEXT,
        user_set_user_filter TEXT,
        user_set_user_filter_args JSON,
        user_set_collection_modules_to_run JSON,
        notifier_params JSON,
        automatic_launch BOOL,
        run_flags JSON,
        extra_params JSON )
        """

user_set_configuration_insert = """
    INSERT INTO user_set_configuration (
        user_set_name,
        user_set_creation_ts,
        user_set_input_collection,
        user_set_input_collection_fpaths,
        user_set_description,
        user_set_notes,
        user_set_user_filter,
        user_set_user_filter_args,
        user_set_collection_modules_to_run,
        notifier_params,
        automatic_launch,
        run_flags,
        extra_params
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING user_set_id
    """

user_set_configuration_update = """
    UPDATE user_set_configuration
    SET user_set_input_collection = %s,
        user_set_input_collection_fpaths = %s,
        user_set_description = %s,
        user_set_notes = %s,
        user_set_user_filter = %s,
        user_set_user_filter_args = %s,
        user_set_collection_modules_to_run = %s,
        notifier_params = %s,
        run_flags = %s,
        extra_params = %s
    WHERE user_set_id = %s
    """

user_set_configuration_fetch = """
    SELECT *
    FROM user_set_configuration
    WHERE user_set_id = {}
    """

user_set_metadata_create = """
CREATE TABLE IF NOT EXISTS user_set_collection_metadata (
    user_set_id INT,
    user_set_collection_start_ts TIMESTAMP,
    user_set_collection_end_ts TIMESTAMP,
    user_set_collection_type TEXT,
    user_set_additional_info JSON,
    PRIMARY KEY (user_set_id, user_set_collection_start_ts, user_set_collection_end_ts, user_set_collection_type))
    """

user_set_metadata_insert = """
    INSERT INTO user_set_collection_metadata (user_set_id, user_set_collection_start_ts, user_set_collection_end_ts, user_set_collection_type, user_set_additional_info) VALUES (%s, %s, %s, %s, %s)
    """


user_tweet_collection_stats_create = """
    CREATE TABLE IF NOT EXISTS {}_user_tweet_collection_stats (
        user_set_id TEXT,
        user_id BIGINT,
        first_collection_bucket_ts TIMESTAMP,
        last_collection_bucket_ts TIMESTAMP,
        total_tweet_count INT,
        original_tweet_count INT,
        retweet_count INT,
        quoted_tweet_count INT,
        reply_count INT,
        first_tweet_ts TIMESTAMP,
        last_tweet_ts TIMESTAMP,
        PRIMARY KEY (user_set_id, user_id) )
        """

user_tweet_collection_stats_fetch = """
    SELECT *
    FROM {}_user_tweet_collection_stats
    """

user_tweet_collection_stats_clear = """
    DELETE
    FROM {}_user_tweet_collection_stats
    """

user_tweet_collection_stats_insert = """
    INSERT INTO {}_user_tweet_collection_stats (
        user_set_id,
        user_id,
        first_collection_bucket_ts,
        last_collection_bucket_ts,
        total_tweet_count,
        original_tweet_count,
        retweet_count,
        quoted_tweet_count,
        reply_count,
        first_tweet_ts,
        last_tweet_ts
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
