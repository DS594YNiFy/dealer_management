import yaml
import logging
import pandas as pd
import re
from update_db.mysql_connector import get_mysql_connection


def replace_col_names(table_name, columns_str):
    """将中文列名改为英文列名"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if re.compile(r"si_fanruan|so_qingbaotong").search(table_name):
        replacements = config["update_" + table_name.replace("_2", "")]["replacements"]
    else:
        replacements = config["update_model"]["replacements"]
    for chinese_name, english_name in replacements.items():
        columns_str = columns_str.replace(chinese_name, english_name)
    return columns_str


def full_update_table(table_name, pd_data):
    """全量更新"""
    logging.info(f"{table_name}")
    try:
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
        logging.info(f"table {table_name} has been fully updated successfully")
    except Exception as e:
        logging.error(f"an error occurred while fully updating table {table_name}: {e}")


class UpdateDBTable:
    """数据库更新"""

    def __init__(self, table_name):
        self.table_name = table_name
        self.data_method = table_name.replace("_2", "")
        self.file_data = pd.DataFrame()
        self.clean_data = pd.DataFrame()

    def load_data(self):
        """获取 csv 数据"""
        with open("config/config.yaml", "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        config_file = config["update_" + self.data_method]
        file_path = config_file["folder_path"] + config_file["csv_path"]
        self.file_data = pd.read_csv(file_path, keep_default_na=False)
        return self.file_data

    def data_clean(self):
        for col in self.file_data.columns:
            if self.file_data[col].isna().all():
                self.file_data[col] = self.file_data[col].astype(object)
            elif pd.to_numeric(self.file_data[col], errors='coerce').notna().all():
                self.file_data[col] = self.file_data[col].fillna('0')
            else:
                continue
        self.clean_data = self.file_data.where(pd.notna(self.file_data), None)
        return self.clean_data

    def update_table(self):
        """全量更新: 加载 -> 清洗 -> 落库"""
        try:
            self.load_data()
            self.data_clean()
            # TODO: 优化类继承, 改为依赖注入
            full_update_table(self.table_name, self.clean_data)
            logging.info("update_" + self.data_method + ".py run successfully")
        except Exception as e:
            logging.error("update_" + self.data_method + ".py run failed")
            logging.error(e)
            raise
