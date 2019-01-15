import run_functions as rf

""" Setting parameters """

# Postgres Credentials
postgres_credential_fpath = "credentials/techne.creds"
twitter_credential_fpath = "credentials/twitter.creds"
config_fpath = "alaska_earthquake.config"

rf.start_collection(postgres_credential_fpath, config_fpath)
rf.schedule_cron_job(postgres_credential_fpath, twitter_credential_fpath, config_fpath)
rf.run_collection(postgres_credential_fpath, twitter_credential_fpath, config_fpath)
