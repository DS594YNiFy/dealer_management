from update_db.data_updater import UpdateDBTable
import pandas as pd
import numpy as np


class UpdateDBSiFanruan(UpdateDBTable):
    """更新帆软数据"""

    def data_clean(self):
        for col in self.file_data.columns:
            if self.file_data[col].isna().all():
                self.file_data[col] = self.file_data[col].astype(object)
            elif pd.to_numeric(self.file_data[col], errors='coerce').notna().all():
                self.file_data[col] = self.file_data[col].fillna('0')
            else:
                continue
        self.file_data["发货日期"] = self.file_data["发货日期"].replace('', np.nan)
        self.clean_data = self.file_data.where(pd.notna(self.file_data), None)
        return self.clean_data


def update_fanruan(table_name):
    """更新帆软数据"""
    update_si_fanruan = UpdateDBSiFanruan(table_name)
    update_si_fanruan.update_table()
