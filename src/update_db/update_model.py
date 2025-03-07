import logging
import yaml
import pandas as pd
from tool import full_update_table


def load_data_dict():
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


def check_data():
    """存入测试表, 如果成功, 返回 true"""
    # TODO
    return True


def update_model():
    """将 data/model/ 中的数据全量更新到数据库"""
    logging.info("python src/update_db/update_model.py")
    model_dict = load_data_dict()
    if check_data():
        for k, v in model_dict.items():
            full_update_table(k,v)
        logging.info("update_model.py run successfully")
    else:
        logging.error("check './data/model/*.csv'")


def main():
    logging.basicConfig(
        filename="logs/update_model.log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    update_model()


if __name__ == "__main__":
    main()
