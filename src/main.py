import logging
from update_db.update_si_fanruan import update_fanruan
from update_db.update_so_qingbaotong import update_so_qingbaotong
from update_db.update_so_sku_douyin import update_so_sku_douyin


def main():
    table_dict = {
        "帆软": "si_fanruan_2", 
        "情报通": "so_qingbaotong_2", 
        "23项指标": "so_metric_2", 
        "情报通": "so_qingbaotong_2", 
        "抖音SKU": "so_sku_douyin_2", 
        "得物SKU": "so_sku_dewu_2", 
        "淘宝SKU": "so_sku_taobao_2", 
        "天猫SKU": "so_sku_tianmao_2"
        }
    try:
        table_name = table_dict[DATA_NAME]
    except Exception as e:
        logging.error(e)
        print("店铺名称错误")

    data_method = table_name.replace("_2", "")
    logging.basicConfig(
        filename="logs/update_" + data_method + ".log",
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG,
    )
    logging.info("update" + table_name)

    if data_method == "si_fanruan":
        update_fanruan(table_name)
    # elif data_method == "so_qingbaotong":
    #     update_so_qingbaotong(table_name, data_method, UPDATE_STEP)
    # elif data_method == "so_sku_douyin":
    #     update_so_sku_douyin(table_name, data_method, UPDATE_STEP)
    else:
        logging.error("no way to update {DATA_NAME} found")
        print(f"未找到更新 {data_method} 的方法")


if __name__ == "__main__":
    """运行参数设置"""

    # "帆软", "情报通", "23项指标", "情报通", "抖音SKU", "得物SKU", "淘宝SKU", "天猫SKU"
    DATA_NAME = "帆软"
    # DATA_NAME = "23项指标"

    # 需要指定运行步骤: "情报通"
    # UPDATE_STEP = 1
    # UPDATE_STEP = 2
    main()
