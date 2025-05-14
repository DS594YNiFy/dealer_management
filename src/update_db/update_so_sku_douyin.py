from update_db.data_updater import UpdateDBTable


class UpdateDBUSKUDouyin(UpdateDBTable):
    """更新 SKU 抖音数据"""

    def __init__(self, table_name, update_method, update_step):
        super().__init__(table_name, update_method)
        self.update_step = update_step


def update_so_sku_douyin(table_name, update_method):
    """更新抖音 SKU 数据"""
    update_so_sku_douyin = UpdateDBUSKUDouyin(table_name, update_method)
    update_so_sku_douyin.update_table()
