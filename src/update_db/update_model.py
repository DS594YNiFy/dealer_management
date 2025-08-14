import logging
import yaml
import pandas as pd
from update_db.data_updater import UpdateDBTable
from update_db.data_updater import full_update_table


class UpdateDBMOdel(UpdateDBTable):
    """将 data/model/ 中的数据全量更新到数据库"""

    def __init__(self):
        self.data_method = "model"

    def load_data_dict(self):
        """获取 csv 数据"""
        with open("config/config.yaml", "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        folder_path = config["update_model"]["folder_path"]
        csv_list = config["update_model"]["table_list"]
        csv_dict = {}
        try:
            for m in csv_list:
                csv_df = pd.read_csv(folder_path + m + ".csv")
                csv_dict[m] = csv_df.where(pd.notna(csv_df), None)
            return csv_dict
        except Exception as e:
            logging.error("load model")
            logging.error(e)

    def update_table(self):
        """全量更新"""
        try:
            model_dict = self.load_data_dict()
            # self.data_clean()
            for k, v in model_dict.items():
                full_update_table(k,v)
            logging.info("update_" + self.data_method + ".py run successfully")
        except Exception as e:
            logging.error("update_" + self.data_method + ".py run failed")
            logging.error(e)


def update_model():
    """将 data/model/ 中的数据全量更新到数据库"""
    update_model = UpdateDBMOdel()
    update_model.update_table()

