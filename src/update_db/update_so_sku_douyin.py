from update_db.data_updater import UpdateDBTable
import pandas as pd
import yaml
import logging


class UpdateDBUSKUDouyin(UpdateDBTable):
    """更新 SKU 抖音数据"""

    def data_clean(self):
        """
        1. 新增列: 日期
        2. 新增列: 店铺编码
        """
        super().data_clean()
        record_date = self.file_data['统计周期'].str[:10].replace('/', '-', regex=False)
        self.file_data.insert(loc=0, column='日期', value=record_date)
        store_code = self.file_data.iloc[:, 0].str.strip() + "_" + self.file_data.iloc[:, 1].str.upper().str.strip()
        self.file_data.insert(loc=0, column='店铺编码', value=store_code)
        self.clean_data = self.file_data.where(pd.notna(self.file_data), None)
        return self.clean_data
    
    def update_table(self):
        """全量更新"""
        self.load_data()
        self.data_clean()
        print("end")



def update_so_sku_douyin(table_name):
    """更新抖音 SKU 数据"""
    update_so_sku_douyin = UpdateDBUSKUDouyin(table_name)
    update_so_sku_douyin.update_table()
