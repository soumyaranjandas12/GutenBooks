import datetime
import mysql.connector.pooling


db_config = {
    'host': 'localhost',
    'port': '3306',
    'user': 'root',
    'password': 'Root123',
    'database': 'books'
}


class MySqlDB(object):
    def __init__(self):
        self.pool_name = None
        self.pool_size = None
        self.pool = None

    def create_pool(self, pool_name, pool_size):
        """
        Create a connection pool, after created, the request connecting MySQL
        could get a connection from this pool instead of request to create a
        connection.
        """
        self.pool_name = pool_name
        self.pool_size = pool_size
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=self.pool_name,
            pool_size=self.pool_size,
            pool_reset_session=True,
            **db_config)

    @staticmethod
    def close_conn(conn, cursor):
        conn.close()
        cursor.close()

    @staticmethod
    def execute(sql, pool, args=None):
        """
        Execute a sql. It could be with or without args
        :param sql:
        :param pool:
        :param args:
        :return:
        """
        t0 = datetime.datetime.now()
        # get connection from connection pool instead of create one.
        conn = pool.get_connection()
        cursor = conn.cursor()
        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)
        res = cursor.fetchall()
        t1 = datetime.datetime.now()
        print('time Consumed: ', t1 - t0)
        MySqlDB.close_conn(conn, cursor)
        return res

    def check_connection(self):
        """
        check_connection() -> Tests the connection with the Mysql server
        parameter - > None
        """
        try:
            conn = self.pool.get_connection()
            db_info = conn.get_server_info()
            print('Connection to MySQL Version ', db_info)
        except Exception:
            raise 'Mysql DB Connection Error.'


db = MySqlDB()


def init_db():
    global db
    # Create connection pool
    db.create_pool('my_test_api', 20)
    db.check_connection()


def get_instance():
    return db

