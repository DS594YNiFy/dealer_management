import yaml
import pymysql
import logging
import re


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
            else:
                self.cursor.execute(query, params)
            if query.strip().lower().startswith("select"):
                result = self.cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except pymysql.Error as e:
            logging.error("error while executing:\n" + query)
            logging.error(e)
            self.connection.rollback()

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


def replace_col_names(table_name, columns_str):
    """将中文列名改为英文列名"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if re.compile(r"so|d365_si").search(table_name):
        replacements = config["update_" + table_name]["replacements"]
    else:
        replacements = config["update_model"]["replacements"]
    for chinese_name, english_name in replacements.items():
        columns_str = columns_str.replace(chinese_name, english_name)
    return columns_str


def full_update_table(table_name, pd_data):
    """全量更新"""
    logging.info(f"{table_name}")
    mysql_db = get_mysql_connection()
    truncate_query = f"TRUNCATE TABLE {table_name};"
    columns_str = ", ".join([f"{col}" for col in pd_data.columns])
    columns_str = replace_col_names(table_name, columns_str)
    values_placeholder = ", ".join(["%s"] * len(pd_data.columns))
    insert_query = (
        f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholder})"
    )
    insert_params = [tuple(row) for row in pd_data.values]
    mysql_db.execute_query(truncate_query)
    mysql_db.execute_query(insert_query, insert_params)
