import csv

from userids import userfilter

from db_connect import database, SQL

from utilities import notifications

class UsersCSV(userfilter.UserFilter):

    def __init__(self, csv_fpath=None, csv_has_header=False):
        if csv_fpath is None:
            raise ValueError("To init `UsersCSV` class, please pass `csv_path` arg.")
        self.csv_fpath = csv_fpath
        self.csv_has_header = csv_has_header

    def pull_users(self):
        with open(self.csv_fpath) as f:
            reader = csv.reader(f)
            if self.csv_has_header:
                next(reader)
            userids = [row[0] for row in reader]
            return userids
