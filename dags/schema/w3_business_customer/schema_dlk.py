"""
Define table schema for staging
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKAccount(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "customer_id", "mode": "NULLABLE", "type": "string"},
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "group_permission", "mode": "NULLABLE", "type": "string"},
            {"name": "phone_number", "mode": "NULLABLE", "type": "string"},
            {"name": "profile_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "bigint"},
            {"name": "tier", "mode": "NULLABLE", "type": "bigint"},
            {"name": "user_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "wrong_pin_count", "mode": "NULLABLE", "type": "bigint"},
            {"name": "till_number", "mode": "NULLABLE", "type": "string"},
            {"name": "vip", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'customer_id': 'str',
            'id': 'int64',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'group_permission': 'str',
            'phone_number': 'str',
            'profile_id': 'int64',
            'status': 'int64',
            'tier': 'int64',
            'user_id': 'int64',
            'wallet_id': 'int64',
            'wrong_pin_count': 'int64',
            'till_number': 'str',
            'vip': 'int64',
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "customer_id", "mode": "NULLABLE", "type": "string"},
            {"name": "id", "type": "bigint"},
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


class DLKProfile(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "dob", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "address", "mode": "NULLABLE", "type": "string"},
            {"name": "district", "mode": "NULLABLE", "type": "bigint"},
            {"name": "first_name", "mode": "NULLABLE", "type": "string"},
            {"name": "gender", "mode": "NULLABLE", "type": "bigint"},
            {"name": "id_number", "mode": "NULLABLE", "type": "string"},
            {"name": "id_type", "mode": "NULLABLE", "type": "string"},
            {"name": "last_name", "mode": "NULLABLE", "type": "string"},
            {"name": "phone_number", "mode": "NULLABLE", "type": "string"},
            {"name": "precinct", "mode": "NULLABLE", "type": "bigint"},
            {"name": "region", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'dob': 'datetime64[ns]',
            'address': 'str',
            'district': 'int64',
            'first_name': 'str',
            'gender': 'int64',
            'id_number': 'str',
            'id_type': 'str',
            'last_name': 'str',
            'phone_number': 'str',
            'precinct': 'int64',
            'region': 'int64',
            'status': 'str',
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint"},
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

DLK_ACCOUNT = "account"
DLK_PROFILE = "profile"

W3_BUSINESS_CUSTOMER_TABLE_SCHEMA = {
    DLK_ACCOUNT: DLKAccount(DLK_ACCOUNT),
    DLK_PROFILE: DLKProfile(DLK_PROFILE),
}

_ALL_DIM = [
    DLK_ACCOUNT,
    DLK_PROFILE
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
