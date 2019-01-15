from userids import userfilter

from db_connect import database, SQL

class AllUsers(userfilter.UserFilter):

    def __init__(self, user_set_prefix=None, user_set_id=None, db=None):
        if any([arg is None for arg in [user_set_prefix, user_set_id, db]]):
            raise ValueError("To init `AllUsers` class, please pass `user_set_prefix`, `user_set_id`, and `db` args.")
        self.db = db
        self.table_name = "{}_user_tweet_collection_stats".format(user_set_prefix)

    def pull_users(self):
        sql = """
            SELECT user_id
            FROM {}
            WHERE user_set_id = {}
        """.format(self.table_name, self.user_set_id)
        user_rows = self.db.execute_sql(sql, fetch=True)
        return [row[0] for row in rows]
