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
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "tier_code", "mode": "NULLABLE", "type": "string"},
            {"name": "tier_name", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'description': 'str',
            'role_type_id': 'int64',
            'status': 'str',
            'tier_code': 'str',
            'tier_name': 'str',
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


class DLKAccountingWalletTypes(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "accounting_wallet_type_name", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "multiple_wallets", "mode": "NULLABLE", "type": "bit"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'accounting_wallet_type_name': 'str',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'deleted_at': 'datetime64[ns]',
            'deleted_by': 'int64',
            'multiple_wallets': 'bool',
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


class DLKAreas(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "code", "mode": "NULLABLE", "type": "string"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "nationally", "mode": "NULLABLE", "type": "string"},
            {"name": "parent_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "type", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "flag", "mode": "NULLABLE", "type": "string"},
            {"name": "postal_code", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'code': 'str',
            'description': 'str',
            'name': 'str',
            'nationally': 'str',
            'parent_id': 'int64',
            'status': 'str',
            'type': 'int64',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'deleted_at': 'datetime64[ns]',
            'deleted_by': 'int64',
            'flag': 'str',
            'postal_code': 'str',
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


class DLKChannels(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "channel_name", "mode": "NULLABLE", "type": "string"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'channel_name': 'str',
            'description': 'str',
            'status': 'str',
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



class DLKBankWallets(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "address", "mode": "NULLABLE", "type": "string"},
            {"name": "bank_date_of_id", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "bank_number_of_id", "mode": "NULLABLE", "type": "string"},
            {"name": "bank_type_of_id", "mode": "NULLABLE", "type": "string"},
            {"name": "contact_name", "mode": "NULLABLE", "type": "string"},
            {"name": "contact_number", "mode": "NULLABLE", "type": "string"},
            {"name": "department", "mode": "NULLABLE", "type": "string"},
            {"name": "email", "mode": "NULLABLE", "type": "string"},
            {"name": "full_name", "mode": "NULLABLE", "type": "string"},
            {"name": "gender", "mode": "NULLABLE", "type": "bigint"},
            {"name": "position", "mode": "NULLABLE", "type": "string"},
            {"name": "representative_number_of_id", "mode": "NULLABLE", "type": "string"},
            {"name": "representative_type_of_id", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'address': 'str',
            'bank_date_of_id': 'datetime64[ns]',
            'bank_number_of_id': 'str',
            'bank_type_of_id': 'str',
            'contact_name': 'str',
            'contact_number': 'str',
            'department': 'str',
            'email': 'str',
            'full_name': 'str',
            'gender': 'int64',
            'position': 'str',
            'representative_number_of_id': 'str',
            'representative_type_of_id': 'str',
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

class DLKBankAccounts(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
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
DLK_ACCOUNTING_WALLET_TYPES = "accounting_wallet_types"
DLK_AREAS = "areas"
DLK_CHANNELS = "channels"
DLK_BANK_WALLETS = "bank_wallets"
DLK_BANK_ACCOUNTS = "bank_accounts"
DLK_INVOICE = "Temp2"

W3_CORE_MDM_TABLE_SCHEMA = {
    DLK_TIERS: DLKTiers(DLK_TIERS),
    DLK_ACCOUNTING_WALLET_TYPES: DLKAccountingWalletTypes(DLK_ACCOUNTING_WALLET_TYPES),
    DLK_AREAS: DLKAreas(DLK_AREAS),
    DLK_CHANNELS: DLKChannels(DLK_CHANNELS),
    DLK_BANK_WALLETS: DLKBankWallets(DLK_BANK_WALLETS),
    DLK_BANK_ACCOUNTS: DLKBankAccounts(DLK_BANK_ACCOUNTS),
}

_ALL_DIM = [
    DLK_TIERS
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
