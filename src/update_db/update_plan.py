import logging
import yaml
import pandas as pd
from tool import full_update_table


def load_data_dict():
    """获取 csv 数据"""
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    folder_path = config["update_"+update_table]["folder_path"]
    csv_list = config["update_"+update_table]["table_list"]
    csv_dict = {}
    try:
        for c in csv_list:
            csv_df = pd.read_csv(folder_path + c + ".csv")
            csv_dict[c] = csv_df.where(pd.notna(csv_df), None)
        return csv_dict
    except:
        logging.error("load plan")


def check_data():
    """存入测试表, 如果成功, 返回 true"""
    # TODO
    return True


def update_plan():
    """将 data/plan/ 中的数据全量更新到数据库"""
    logging.info("python src/update_db/update_plan.py")
    plan_dict = load_data_dict()
    if check_data():
        for k, v in plan_dict.items():
            full_update_table(k,v)
        logging.info("update_plan.py run successfully")
    else:
        logging.error("check './data/plan/*.csv'")


def main():
    logging.basicConfig(
        filename="logs/update_" + update_table + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    update_plan()


if __name__ == "__main__":
    update_table = "plan"
    update_method = "incremental"
    update_method = "full"
    main()
