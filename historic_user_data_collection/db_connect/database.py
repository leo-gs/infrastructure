import psycopg2
from psycopg2 import extras as ext
from utilities import notifications


class Database:

    def __init__(self, host, user, password, db_name,
                 create_if_not_exists=False, notifiers=[]):

        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name

        connection_str = "host='{host}' user='{user}' password='{password}'".format(
            host=self.host, user=self.user, password=self.password)

        # check if the database already exists
        db_conn = psycopg2.connect(connection_str)
        cursor = db_conn.cursor()
        cursor.execute(
            """
        SELECT EXISTS (
        	SELECT *
        	FROM pg_catalog.pg_database
        	WHERE datname = '{}')
        """.format(db_name))

        db_exists = cursor.fetchall()[0][0]

        cursor.close()
        db_conn.close()

        notifications.notify_all(notifiers, "Database `{}` already exists? {}"
                        .format(db_name, db_exists), notify_type="info")

        if not db_exists:
            if create_if_not_exists:
                notifications.notify_all(notifiers, "Creating database {}....".format(db_name,
                                notify_type="start"))

                db_conn=psycopg2.connect(connection_str)
                db_conn.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                cursor=db_conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))

                db_conn.commit()
                cursor.close()
                db_conn.close()

                notifications.notify_all(notifiers, "Done creating database `{}`".format(db_name), notify_type="complete")

            else:
                raise ValueError("The database {} doesn't exist. If you want"
                                 + " to create it, set `create_if_not_exists` to True.")

    def get_connection_string(self):
        return ("host='{host}' " + "user='{user}' " + "password='{password}' " + "dbname='{db_name}'").format(host=self.host, user=self.user, password=self.password, db_name=self.db_name)

    def get_db_connection(self):
        connection_str=self.get_connection_string()
        return psycopg2.connect(connection_str)


    def execute_sql(self, sql, args=None,
            commit=False, fetch=False, batch_insert=False):

        db_conn=self.get_db_connection()
        cursor=db_conn.cursor()

        if batch_insert:
            ext.execute_batch(cursor, sql, args)
        else:
            if args:
                cursor.execute(sql, args)
            else:
                cursor.execute(sql)

        retval=None
        if fetch:
            retval=cursor.fetchall()

        if commit:
            db_conn.commit()

        cursor.close()
        db_conn.close()

        return retval
