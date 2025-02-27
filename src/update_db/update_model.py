import logging
import yaml
import pandas as pd
import pymysql


def load_model():
    """获取 model 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_model"]["folder_path"]
    model_list = config["update_model"]["model_list"]
    model_dict = {}
    try:
        for m in model_list:
            df = pd.read_csv(folder_path + m + ".csv")
            model_dict[m] = df.where(pd.notna(df), None)
        return model_dict
    except:
        logging.error("load model")


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


def check_model_data():
    """存入测试表, 如果成功, 返回 true"""
    # TODO
    return True


def replace_col_names(columns_str):
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    replacements = config["update_model"]["replacements"]
    for chinese_name, english_name in replacements.items():
        columns_str = columns_str.replace(chinese_name, english_name)
    return columns_str


def save_model(model_dict):
    """更新数据库"""
    for k, v in model_dict.items():
        logging.info(f"{k}")
        mysql_db = get_mysql_connection()
        truncate_query = f"TRUNCATE TABLE {k};"
        columns_str = ', '.join([f'{col}' for col in v.columns])
        columns_str = replace_col_names(columns_str)
        values_placeholder = ', '.join(['%s'] * len(v.columns))
        insert_query = f'INSERT INTO {k} ({columns_str}) VALUES ({values_placeholder})'
        insert_params = [tuple(row) for row in v.values]
        mysql_db.execute_query(truncate_query)
        mysql_db.execute_query(insert_query, insert_params)


def main():
    """全量更新 data/model/ 中的数据到数据库"""
    logging.basicConfig(
        filename="logs/update_model.log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    logging.info("update model")
    model_dict = load_model()
    if check_model_data():
        save_model(model_dict)
        logging.info("update model successfully")
    else:
        logging.error("check './data/model/*.csv'")


if __name__ == "__main__":
    main()
    print()
