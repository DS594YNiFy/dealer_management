import logging
import yaml
import os
import re
import pandas as pd
from .base import full_update_table


def load_data():
    """获取 csv 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_" + update_table]["folder_path"]
    pattern = r"^DynamicsExport_\d+\(1\).xlsx$"
    for filename in os.listdir(folder_path):
        if re.match(pattern, filename):
            try:
                xls_df = pd.read_excel(folder_path + filename)
            except Exception as e:
                logging.error(f"load d365_si: {e}")
        else:
            continue
    xls_df.to_csv(folder_path + update_table + "_2.csv", index=False, encoding='utf-8-sig')
    csv_df = pd.read_csv(folder_path + update_table + "_2.csv")
    df = csv_df.where(pd.notna(csv_df), None)
    return df


def incremental_update_table(update_table):
    return


def update_d365_si():
    """将 data/d365_si/ 中的数据增量或全量更新到数据库"""
    logging.info("python src/update_db/update_" + update_table + ".py")
    logging.info(f"update_method: {update_method}")
    if update_method == "full":
        csv_data = load_data()
        full_update_table(update_table + "_2", csv_data)
        logging.info("update_" + update_table + ".py run successfully")
    elif update_method == "incremental":
        # FIXME: 筛选 d365_si_2 中的数据并存入 d365_si
        incremental_update_table(update_table)
    else:
        logging.error("update_" + update_table + ".py run failed")


def main():
    logging.basicConfig(
        filename="logs/update_" + update_table + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    update_d365_si()


if __name__ == "__main__":
    update_table = "d365_si"
    update_method = "incremental"
    update_method = "full"
    main()
