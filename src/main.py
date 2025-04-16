import logging
from update_db.data_updater import UpdateDBTable


class UpdateDBSiFanruan(UpdateDBTable):
    """更新帆软数据"""
    pass


# class UpdateDBSo(UpdateDBTable):
#     """更新平台数据"""

#     def __init__(self, table_name, update_method):
#         super().__init__(table_name, update_method)

#     def data_clean(csv_data):
#         """清洗 GMV = 0 或 is null 的数据"""
#         valid_gmv = (csv_data["gmv".upper()] != 0) & ~csv_data["gmv".upper()].isna()
#         clean_data = csv_data[valid_gmv]
#         return clean_data


def main():
    data_method = update_table.replace("_2", "")
    logging.basicConfig(
        filename="logs/update_" + data_method + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    logging.info("update" + update_table)

    # update fanruan
    update_si_fanruan = UpdateDBSiFanruan(update_table, data_method)
    update_si_fanruan.update_table()


if __name__ == "__main__":
    # si_fanruan_2
    update_table = "si_fanruan_2"
    main()
