import pymysql

"""
Database class loads the database credentials
This class is used to create a pymysql connector to connect to the database
For more reference on pymysql: http://pymysql.readthedocs.io/en/latest/index.html
"""


class Database(object):
    def __init__(self, host_name, database_name, user_name, password, charset=None):
        """
        :param host_name: Database Host Name or Server Name
        :param database_name: Name of the database to connect to
        :param user_name: User credential for authorization
        :param password: Password for authorization
        :param charset: Character set for the database
        """
        self.host_name = host_name
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.charset = charset

    def connect_with_pymysql(self, unicode=True):
        """
        :param unicode: Set it to true if you're working with unicodes
        :return: Returns a pymysql.connect object on success, else prints connection error and returns None
        """
        try:
            connect = pymysql.connect(
                host=self.host_name,
                user=self.user_name,
                passwd=self.password,
                db=self.database_name,
                use_unicode=unicode,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            return connect
        except Exception:
            print "Connection error"
