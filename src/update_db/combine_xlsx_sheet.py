import logging
import yaml
import os
import re
import pandas as pd
from .base import full_update_table


def combine_xlsx_sheet():
    """获取 xlsx 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    xlsx_path = config["combine_xlsx_sheet"]["input_path"]
    all_sheets = pd.read_excel(xlsx_path, sheet_name=None)
    combined = pd.concat(all_sheets.values(), ignore_index=True)
    store_code = combined.iloc[:, 0] + "_" + combined.iloc[:, 1].str.upper()
    combined.insert(loc=0, column='store_code', value=store_code)
    csv_path = config["combine_xlsx_sheet"]["output_path"]
    combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return combined


def data_clean(csv_data):
    """清洗 CSV 数据"""
    valid_gmv = (csv_data["gmv".upper()] != 0) & ~csv_data["gmv".upper()].isna()
    so_data = csv_data[valid_gmv]
    return so_data


def update_combine_xlsx_sheet():
    """将 data/so/ 中的数据增量或全量更新到数据库"""
    logging.info("python src/update_db/" + update_table + ".py")
    try:
        csv_data = combine_xlsx_sheet()
        db_data = data_clean(csv_data)
        full_update_table(update_table, db_data)
        logging.info(update_table + ".py run successfully")
    except Exception as e:
        logging.error(update_table + ".py run failed")
        logging.error(e)


def main():
    logging.basicConfig(
        filename="logs/update_" + update_table + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    combined = combine_xlsx_sheet()
    combined


if __name__ == "__main__":
    update_table = "combine_xlsx_sheet"
    main()
