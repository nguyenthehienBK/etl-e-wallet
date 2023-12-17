"""
Define table schema for staging and warehouse
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.lakehouse.table_utils import get_content_from_sql_path
from utils.type.data_type import (
    RAW_TYPE_DATETIME
    ,RAW_TYPE_INT64
    ,RAW_TYPE_INT
    ,RAW_TYPE_STR
)


class DLKUserWallets(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "role_type_ref_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'role_type_ref_id': 'int64',
            'wallet_id': 'int64',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'deleted_at': 'datetime64[ns]',
            'deleted_by': 'int64',
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint"}
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


class DLKUserRoleTypeRef(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "user_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "role_type_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "tier_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "phone_number_otp", "mode": "NULLABLE", "type": "string"},
            {"name": "pin", "mode": "NULLABLE", "type": "string"},
            {"name": "password", "mode": "NULLABLE", "type": "string"},
            {"name": "count_invalid_otp", "mode": "NULLABLE", "type": "int"},
            {"name": "count_invalid_pin", "mode": "NULLABLE", "type": "int"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "client_id", "mode": "NULLABLE", "type": "string"},
            {"name": "last_ip_address", "mode": "NULLABLE", "type": "string"},
            {"name": "count_invalid_pw", "mode": "NULLABLE", "type": "int"},
            {"name": "last_login_device_id", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': RAW_TYPE_INT64,
            'user_id': RAW_TYPE_INT64,
            'role_type_id': RAW_TYPE_INT64,
            'tier_id': RAW_TYPE_INT64,
            'status': RAW_TYPE_STR,
            'phone_number_otp': RAW_TYPE_STR,
            'pin': RAW_TYPE_STR,
            'password': RAW_TYPE_STR,
            'count_invalid_otp': RAW_TYPE_INT,
            'count_invalid_pin': RAW_TYPE_INT, 
            'created_at': RAW_TYPE_DATETIME,
            'created_by': RAW_TYPE_INT64,
            'updated_at': RAW_TYPE_DATETIME,
            'updated_by': RAW_TYPE_INT64,
            'deleted_at': RAW_TYPE_DATETIME,
            'deleted_by': RAW_TYPE_INT64,
            'client_id': RAW_TYPE_STR,
            'last_ip_address': RAW_TYPE_STR,
            'count_invalid_pw': RAW_TYPE_INT,
            'last_login_device_id': RAW_TYPE_INT64
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint"}
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
        self.WRAP_CHAR = ''


class DLKUsers(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "first_name", "mode": "NULLABLE", "type": "string"},
            {"name": "last_name", "mode": "NULLABLE", "type": "string"},
            {"name": "username", "mode": "NULLABLE", "type": "string"},
            {"name": "email", "mode": "NULLABLE", "type": "string"},
            {"name": "phone_number", "mode": "NULLABLE", "type": "string"},
            {"name": "birthday", "mode": "NULLABLE", "type": "datetime"},
            {"name": "country", "mode": "NULLABLE", "type": "bigint"},
            {"name": "image_url", "mode": "NULLABLE", "type": "string"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "lang", "mode": "NULLABLE", "type": "bigint"},
            {"name": "sex", "mode": "NULLABLE", "type": "string"},
            {"name": "time_zone", "mode": "NULLABLE", "type": "string"},
            {"name": "reset_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "reset_key", "mode": "NULLABLE", "type": "string"},
            {"name": "activation_key", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"}
        ]
        self.SCHEMA_RAW = {
            'id': RAW_TYPE_INT64,
            'first_name': RAW_TYPE_STR,
            'last_name': RAW_TYPE_STR,
            'username': RAW_TYPE_STR,
            'email': RAW_TYPE_STR,
            'phone_number': RAW_TYPE_STR,
            'birthday': RAW_TYPE_DATETIME,
            'country': RAW_TYPE_INT64,
            'image_url': RAW_TYPE_STR,
            'status': RAW_TYPE_STR,
            'lang': RAW_TYPE_INT64,
            'sex': RAW_TYPE_STR,
            'time_zone': RAW_TYPE_DATETIME,
            'reset_date': RAW_TYPE_DATETIME,
            'reset_key': RAW_TYPE_STR,
            'activation_key': RAW_TYPE_STR,
            'created_at': RAW_TYPE_DATETIME,
            'created_by': RAW_TYPE_INT64,
            'updated_at': RAW_TYPE_DATETIME,
            'updated_by': RAW_TYPE_INT64,
            'deleted_at': RAW_TYPE_DATETIME,
            'deleted_by': RAW_TYPE_INT64
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint"}
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
        self.WRAP_CHAR = ''

_ALL = "all"
""" ALL table name in database """

DLK_USER_WALLETS = "user_wallets"
DLK_USER_ROLE_TYPE_REF = "user_role_type_ref"
DLK_USERS = "users"

W3_CORE_UAA_TABLE_SCHEMA = {
    DLK_USER_WALLETS: DLKUserWallets(DLK_USER_WALLETS),
    DLK_USER_ROLE_TYPE_REF: DLKUserRoleTypeRef(DLK_USER_ROLE_TYPE_REF),
    DLK_USERS: DLKUsers(DLK_USERS)
}

_ALL_DIM = [
    DLK_USER_WALLETS
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
