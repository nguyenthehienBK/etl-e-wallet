"""
Define table schema for staging
File dags/schema/kv_mssql/schema_dlk.py
"""
from airflow.models import Variable
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.database.db_data_type import UpsertType
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKTiers(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"}
            , {"name": "description", "mode": "NULLABLE", "type": "string"}
            , {"name": "role_type_id", "mode": "NULLABLE", "type": "bigint"}
            , {"name": "status", "mode": "NULLABLE", "type": "string"}
            , {"name": "tier_code", "mode": "NULLABLE", "type": "string"}
            , {"name": "tier_name", "mode": "NULLABLE", "type": "string"}
            , {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"}
            , {"name": "created_by", "mode": "NULLABLE", "type": "bigint"}
            , {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"}
            , {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"}
            , {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"}
            , {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"}
        ]
        self.SCHEMA_RAW = {
            'id': 'int64'
            , 'description': 'str'
            , 'role_type_id': 'int64'
            , 'status': 'str'
            , 'tier_code': 'str'
            , 'tier_name': 'str'
            , 'created_at': 'datetime64[ns]'
            , 'created_by': 'int64'
            , 'updated_at': 'datetime64[ns]'
            , 'updated_by': 'int64'
            , 'deleted_at': 'datetime64[ns]'
            , 'deleted_by': 'int64'
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "Id", "type": "bigint"}
        ]
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "id",
            "JOIN": ""
        }


_ALL = "all"
""" ALL table name in database """

DLK_TIERS = "tiers"
DLK_INVOICE = "Temp2"

W3_CORE_MDM_TABLE_SCHEMA = {
    DLK_TIERS: DLKTiers(DLK_TIERS),
}

_ALL_DIM = [
    DLK_TIERS
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
