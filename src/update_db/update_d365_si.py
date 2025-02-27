import logging
import yaml
import pandas as pd
from tool import full_update_table


def load_data():
    """获取 csv 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_"+update_table]["folder_path"]
    try:
        df = pd.read_csv(folder_path + m + ".csv")
        df = df.where(pd.notna(df), None)
        return df
    except:
        logging.error("load model")


def check_model_data():
    """存入测试表, 如果成功, 返回 true"""
    # TODO
    return True


def update_d365_si():
    """将 data/d365_si/ 中的数据增量或全量更新到数据库"""
    logging.info("python src/update_db/update_"+update_table+".py")
    logging.info(f"update_method: {update_method}")
    if update_method == "full":
        csv_data = load_data()
        check_model_data()
        full_update_table(update_table,csv_data)
        logging.info("update_"+update_table+".py run successfully")
    else:
        logging.error("check './data/"+update_table+"/*.csv'")


def main():
    logging.basicConfig(
        filename="logs/update_"+update_table+".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    update_d365_si()


if __name__ == "__main__":
    update_method = "incremental"
    update_method = "full"
    update_table = "d365_si"
    main()
