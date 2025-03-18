import logging
import yaml
import pandas as pd
import sys
import os

tool_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(tool_dir)

from update_db.tool import get_mysql_connection


def fetch_data(select_query):
    mysql_db = get_mysql_connection()
    select_query = select_query[select_query.find("\n") + 1 :].strip("\n")
    db_data = mysql_db.execute_query(select_query)
    df = pd.DataFrame(db_data)
    return df


def main():
    logging.basicConfig(
        filename="logs/get_data.log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    with open("config/config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    query_path = config["get_mysql_data"]["query_path"]
    with open(query_path, "r", encoding="utf-8") as file:
        query = file.read()
    db_data = fetch_data(query)
    output_path = config["get_mysql_data"]["output_path"]
    df = pd.DataFrame(db_data)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
