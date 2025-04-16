import logging
import yaml
import pandas as pd
from src.update_db.data_updater import full_update_table


def load_data(data_method):
    """获取 csv 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_" + data_method]["folder_path"]
    csv_df = pd.read_csv(folder_path + update_table + ".csv")
    df = csv_df.where(pd.notna(csv_df), None)
    return df


def data_clean(csv_data):
    """清洗 CSV 数据"""
    valid_gmv = (csv_data["gmv".upper()] != 0) & ~csv_data["gmv".upper()].isna()
    clean_data = csv_data[valid_gmv]
    return clean_data


def update_so(data_method):
    """将 data/so/ 中的数据增量或全量更新到数据库"""
    logging.info("python src/update_db/update_" + update_table + ".py")
    try:
        if update_table in ["so", "so_2"]:
            csv_data = load_data(data_method)
            db_data = data_clean(csv_data)
            full_update_table(update_table, db_data)
            logging.info("update_" + data_method + ".py run successfully")
        else:
            logging.error(f"update_table = {update_table}, table name error")
    except Exception as e:
        logging.error("update_" + data_method + ".py run failed")


def main():
    data_method = update_table.replace("_2", "")
    logging.basicConfig(
        filename="logs/update_" + data_method + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    update_so(data_method)


if __name__ == "__main__":
    update_table = "so"
    # update_table = "so_2"
    main()
