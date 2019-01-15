from datetime import datetime

def twitter_str_to_dt(dt_str):
    return datetime.strptime(dt_str, "%a %b %d %H:%M:%S +0000 %Y")
