import json
import sys

import run_functions as rf


if __name__ == "__main__":
    config_fpath = sys.argv[1]

    with open(config_fpath) as f:
        config = json.load(f)

    if "user_set_id" not in config:
        rf.initialize_collection(config_fpath)

    day_interval = config["day_interval"]
    rf.schedule_cron_job(config_fpath, every_x_days=day_interval)
    rf.run_collection(config_fpath)
