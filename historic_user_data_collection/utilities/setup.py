from collection import *
from db_connect import database, SQL
from utilities import notifications
from userids import userscsv

def server_setup(host, user, password, db_name, notifiers):

    notifications.notify_all(notifiers, "Beginning data collection setup...",
               notify_type="start")

    try:
        db = database.Database(host, user, password, db_name,
                            create_if_not_exists=True)

        # Create the tables if they don't exist
        db.execute_sql(SQL.user_set_configuration_create, commit=True)
        db.execute_sql(SQL.user_set_metadata_create, commit=True)

        notifications.notify_all(notifiers, "Finished server setup!",
                   notify_type="complete")

        return db

    except Exception as e:
        notifications.notify_all(notifiers, "Server setup failed!",
                   notify_type="error")
        notifications.notify_all(notifiers, str(e), notify_type="error")
        raise e


def setup_userid_module(user_set_prefix, module_name, module_args, notifiers):
    notifications.notify_all(notifiers, "Created '{}' filter".format(
        module_name), notify_type="info")
    module_class = eval("{}".format(module_name))

    module = module_class(**module_args)
    return module


def setup_collection_module(user_set_id, user_set_prefix, module_name, module_args, db, notifiers):
    module_class = eval("{}".format(module_name))
    module = module_class(user_set_id=user_set_id, user_set_prefix=user_set_prefix, db=db, notifiers=notifiers, **module_args)
    return module
