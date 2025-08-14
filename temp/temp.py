import yaml
import logging
import pandas as pd
import re
from typing import List, Dict, Optional, Tuple
from update_db.mysql_connector import get_mysql_connection


class BaseDBUpdater:
    """通用的“读 -> 清洗 -> 全量落库”基类。子类可按需重写钩子方法。"""

    CONFIG_PATH = "config/config.yaml"

    def __init__(self, table_name: str, config_path: Optional[str] = None):
        self.table_name = table_name
        self.data_method = table_name.replace("_2", "")
        self.file_data = pd.DataFrame()
        self.clean_data = pd.DataFrame()
        self._config_path = config_path or self.CONFIG_PATH
        self._config_cache: Optional[dict] = None

    # ---------- 配置与映射 ----------

    def load_config(self) -> dict:
        if self._config_cache is None:
            with open(self._config_path, "r", encoding="utf-8") as f:
                self._config_cache = yaml.safe_load(f)
        return self._config_cache

    def get_replacements(self) -> Dict[str, str]:
        """
        返回“中文列名 -> 英文列名”的映射。
        子类若有特殊规则可重写本方法。
        """
        cfg = self.load_config()
        if re.compile(r"si_fanruan|so_qbt").search(self.table_name):
            return cfg["update_" + self.data_method]["replacements"]
        return cfg["update_model"]["replacements"]

    def map_columns(self, cols: List[str]) -> List[str]:
        """
        用映射字典逐列转换；不存在于映射中的列名保持原样。
        """
        repl = self.get_replacements()
        return [repl.get(c, c) for c in cols]

    # ---------- IO ----------

    def load_data(self) -> pd.DataFrame:
        """获取 CSV 数据；子类可重写以支持多源。"""
        cfg = self.load_config()
        conf = cfg["update_" + self.data_method]
        file_path = conf["folder_path"] + conf["csv_path"]
        # 注：keep_default_na=False 会把空串保留为 ""，后续可统一转 None
        self.file_data = pd.read_csv(file_path, keep_default_na=False)
        return self.file_data

    # ---------- 清洗 ----------

    def data_clean(self) -> pd.DataFrame:
        """
        简单清洗：
        - 能全列安全转数值的列，转为数值并以 0 填充缺失
        - 其它列把空串/NaN 统一为 None，便于 DB NULL
        """
        df = self.file_data.copy()

        for col in df.columns:
            # 试探整列是否“可数值化”
            numeric_probe = pd.to_numeric(df[col], errors="coerce")
            if numeric_probe.notna().all():
                df[col] = numeric_probe.fillna(0)
            else:
                # 对对象列，把空串标准化为 NaN
                df[col] = df[col].replace("", pd.NA)

        # 最终把缺失值变 None（让 DB 驱动写 NULL）
        self.clean_data = df.where(pd.notna(df), None)
        return self.clean_data

    # ---------- SQL 落库 ----------

    def build_insert_sql(self, cols: List[str]) -> Tuple[str, str]:
        """
        构建 INSERT 语句与占位符串。
        - 列名加反引号，避免与关键字/特殊字符冲突
        """
        quoted_cols = ", ".join(f"`{c}`" for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO `{self.table_name}` ({quoted_cols}) VALUES ({placeholders})"
        return insert_sql, placeholders

    def full_update_table(self, df: pd.DataFrame) -> None:
        """
        全量更新：TRUNCATE 后批量 INSERT。
        你的 get_mysql_connection() 若支持 executemany，就直接传 List[tuple]。
        """
        logging.info(f"[full_update] {self.table_name}")

        mysql_db = get_mysql_connection()

        # 映射列名并按顺序构造 SQL
        mapped_cols = self.map_columns(list(df.columns))
        insert_sql, _ = self.build_insert_sql(mapped_cols)

        # 注意：用 itertuples 比 values 转 tuple 更稳（避免 numpy 类型坑）
        params = list(df.itertuples(index=False, name=None))

        try:
            mysql_db.execute_query(f"TRUNCATE TABLE `{self.table_name}`;")
            # 大表可考虑分批：每 10k 或 50k 行一批
            mysql_db.execute_query(insert_sql, params)
            logging.info(f"table `{self.table_name}` fully updated successfully")
        except Exception as e:
            logging.error(f"error while fully updating `{self.table_name}`: {e}")
            raise

    # ---------- 对外入口 ----------

    def update_table(self) -> None:
        """完整流程：加载 -> 清洗 -> 落库"""
        try:
            self.load_data()
            self.data_clean()
            self.full_update_table(self.clean_data)
            logging.info(f"update_{self.data_method}.py run successfully")
        except Exception as e:
            logging.error(f"update_{self.data_method}.py run failed")
            logging.error(e)
            # 保持抛出，让上层感知失败（可选）
            raise


# ====== 示例：保留你原来的类名，直接继承基类 ======
class UpdateDBTable(BaseDBUpdater):
    """
    若与基类一致，什么都不用写。
    如果某些表的列名映射或数据源路径不同，重写 get_replacements()/load_data() 即可。
    """
    pass
