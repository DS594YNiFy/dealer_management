import logging
import yaml
import os
import re
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from base import full_update_table


def load_qbt_xlsx(folder_path):
    """获取 xlsx 数据"""
    last_month = datetime.now() - relativedelta(months=1)
    date_str = last_month.strftime("%Y%m")
    pattern = fr'情报通_{date_str}_guwen_.*\.xlsx'
    try: 
        for file_name in os.listdir(folder_path):
            if re.match(pattern, file_name):
                xlsx_file = folder_path + file_name
            else:
                continue
        return xlsx_file, date_str
    except FileNotFoundError:
        logging.error(f"qbt.xlsx not found: {e}")
    except Exception as e:
        print(f"an unexpected error occurred: {e}")


def load_sheets(xlsx_file):
    """合并 sheet"""
    all_sheets = {}
    try:
        all_sheets['tmall'] = pd.read_excel(xlsx_file, sheet_name="天猫")
        all_sheets['taobao'] = pd.read_excel(xlsx_file, sheet_name="淘宝")
        all_sheets['jd'] = pd.read_excel(xlsx_file, sheet_name="京东")
        all_sheets['douyin'] = pd.read_excel(xlsx_file, sheet_name="抖音")
        all_sheets['poizon'] = pd.read_excel(xlsx_file, sheet_name="得物")
        all_sheets['pinduoduo'] = pd.read_excel(xlsx_file, sheet_name="拼多多")
        all_sheets['vipshop'] = pd.read_excel(xlsx_file, sheet_name="唯品会")
        all_sheets['funqile'] = pd.read_excel(xlsx_file, sheet_name="分期乐")
        all_sheets['kaola'] = pd.read_excel(xlsx_file, sheet_name="网易考拉")
        all_sheets['xiaohongshu'] = pd.read_excel(xlsx_file, sheet_name="小红书")
        all_sheets['bilibili'] = pd.read_excel(xlsx_file, sheet_name="bilibili")
        all_sheets['suning'] = pd.read_excel(xlsx_file, sheet_name="苏宁易购")
        all_sheets['weidian'] = pd.read_excel(xlsx_file, sheet_name="微店")
        all_sheets['weishi'] = pd.read_excel(xlsx_file, sheet_name="微视")
    except Exception as e:
        logging.error(f"load qbt.xlsx[sheet] failed: {e}")
    all_sheets_data = pd.concat(all_sheets.values(), ignore_index=True)
    return all_sheets_data


def format_col(xlsx_data):
    """日期列名转换"""
    col_str_1 = xlsx_data.columns[:8].tolist()
    col_dates = xlsx_data.columns[8:8+48].tolist()
    col_str_2 = xlsx_data.columns[8+48:].tolist()
    try:
        converted_dates = pd.to_datetime(col_dates, unit='D', origin='1899-12-30')
        col_dates = [f"{d.year}-{d.month}-{d.day}" for d in converted_dates]
    except Exception as e:
        logging.error(f"qbt.xlsx column name conversion failed: {e}")
    xlsx_data.columns = col_str_1 + col_dates + col_str_2
    return xlsx_data


def load_xlsx_and_combine():
    """获取 xlsx 数据, 并和并 sheet"""
    logging.info("python src/update_db/" + update_table + ".py")
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_"+update_table]["folder_path"]
    xlsx_file, date_str = load_qbt_xlsx(folder_path)
    xlsx_data = load_sheets(xlsx_file)
    xlsx_data = format_col(xlsx_data)
    try:
        store_code = xlsx_data.iloc[:, 0].str.strip() + "_" + xlsx_data.iloc[:, 1].str.upper().str.strip()
        xlsx_data.insert(loc=0, column='店铺编码', value=store_code)
        csv_path = folder_path + f"qbt_{date_str}.csv"
        xlsx_data.to_csv(csv_path, index=False, encoding="utf-8-sig")
        xlsx_data.to_csv(folder_path+'_2.csv', index=False, encoding="utf-8-sig")
        return xlsx_data
    except Exception as e:
        logging.error(f"output qbt.csv failed: {e}")


def load_data():
    """获取 csv 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_" + update_table]["folder_path"]
    csv_df = pd.read_csv(folder_path + update_table + "_2.csv")
    df = csv_df.where(pd.notna(csv_df), None)
    return df


def data_clean(csv_data):
    """清洗 CSV 数据"""
    valid_gmv = (csv_data["gmv".upper()] != 0) & ~csv_data["gmv".upper()].isna()
    clean_data = csv_data[valid_gmv]
    return clean_data


def incremental_update_table(update_table):
    return


def update_qbq():
    """将 data/so/ 中的数据增量或全量更新到数据库"""
    logging.info("python src/update_db/update_" + update_table + ".py")
    logging.info(f"update_method: {update_method}")
    if update_method == "full":
        csv_data = load_data()
        db_data = data_clean(csv_data)
        full_update_table(update_table + "_2", db_data)
        logging.info("update_" + update_table + ".py run successfully")
    elif update_method == "incremental":
        # FIXME: 筛选 so_2 中的数据并存入 so
        incremental_update_table(update_table)
    else:
        logging.error("update_" + update_table + ".py run failed")


def main():
    logging.basicConfig(
        filename="logs/update_" + update_table + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    if step == 1:
        load_xlsx_and_combine()
    elif step == 2:
        update_qbq()
    else:
        print("ERROR: step error")


if __name__ == "__main__":
    update_table = "qbt"
    # update_method = "incremental"
    update_method = "full"
    # step = 1
    step = 2
    main()
