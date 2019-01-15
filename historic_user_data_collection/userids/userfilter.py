from db_connect import database, SQL

class UserFilter:

    def __init__(self):
        raise NotImplemented("To make a user filter, please implement a custom class.")

    ## Override this method in custom class
    def pull_users(self):
        raise NotImplemented("To pull user_ids, please implement `pull_users` in a custom class.")
