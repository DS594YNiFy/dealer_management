import yaml
import pymysql
import logging


class MySQLDatabase:
    """数据库接口"""

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def mysql_connection(self):
        """获取 mysql 连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            self.cursor = self.connection.cursor()
        except pymysql.Error as e:
            logging.error(f"error while connecting to MySQL: {self.database}")
            logging.error(e)

    def execute_query(self, query, params=None):
        """执行 SQL 语句"""
        if not self.connection or self.connection.open is False:
            self.mysql_connection()
        try:
            if params and isinstance(params[0], tuple):
                rows_affected = self.cursor.executemany(query, params)
                logging.info(f"rows_affected: {rows_affected}")
                self.connection.commit()
                return self.cursor.rowcount
            elif params:
                self.cursor.execute(query, params)
                self.connection.commit()
                return self.cursor.rowcount
            elif query.strip().lower().startswith("select"):
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                return result
            else:
                self.cursor.execute(query)
                self.connection.commit()
                return self.cursor.rowcount
        except pymysql.Error as e:
            self.connection.rollback()
            logging.error("error while executing:\n" + query)
            logging.error(e)
            return None

    def close(self):
        """关闭 MySQL 数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def get_mysql_connection():
    """连接 MySQL 数据库"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    db_info = config["database"]
    mysql_db = MySQLDatabase(
        host=db_info["host"],
        user=db_info["user"],
        password=db_info["password"],
        database=db_info["database"],
    )
    mysql_db.mysql_connection()
    return mysql_db
