import os

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import pool
import my_logger


class DatabaseConnection:

    def __init__(self):
        self.logger = my_logger.MyLogger(__name__).logger
        minconn = 1
        maxconn = 50
        print('init db_connection.py')
        host = os.getenv('POSTGRES_HOST')
        port = os.getenv('POSTGRES_PORT')
        database = os.getenv('POSTGRES_DB')
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        # self.connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        # self.cursor = self.connection.cursor(cursor_factory=DictCursor)
        # コネクションプールの作成
        self.connection_pool = pool.ThreadedConnectionPool(minconn, maxconn,
                                                           host=host,
                                                           port=port,
                                                           database=database,
                                                           user=user,
                                                           password=password)

    def get_connection(self):
        self.logger.debug('get_connection')
        connection = self.connection_pool.getconn()
        cursor = connection.cursor(cursor_factory=DictCursor)
        return connection, cursor

    def close_connection(self, connection, cursor):
        self.logger.debug('close_connection')
        cursor.close()
        self.connection_pool.putconn(connection)

    def select_one(self, sql, params=None):
        connection, cursor = self.get_connection()
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        result = cursor.fetchone()[0]
        self.close_connection(connection, cursor)
        return result

    def select_all(self, sql, params=None):
        self.logger.info('select_all')
        connection, cursor = self.get_connection()
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        result = cursor.fetchall()
        self.close_connection(connection, cursor)
        return result

    def execute(self, sql, params):
        self.logger.debug('execute')
        connection, cursor = self.get_connection()
        cursor.execute(sql, params)
        connection.commit()
        self.logger.debug('execute commit')
        self.close_connection(connection, cursor)

    def no_commit_execute(self, sql, params):
        connection, cursor = self.get_connection()
        cursor.execute(sql, params)

    def commit(self):
        self.connection.commit()
