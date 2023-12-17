"""
Define table schema for staging
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.type.data_type import *
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKWallet(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "pan", "mode": "NULLABLE", "type": "string"},
            {"name": "wallet_state_id", "mode": "NULLABLE", "type": "int"},
            {"name": "wallet_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "modified_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "active_time", "mode": "NULLABLE", "type": "datetime"},    # 20231210: Không tìm thấy trong [NMS] Thông tin yêu cầu DB 
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "customer_id", "mode": "NULLABLE", "type": "string"},      # 20231210: Không tìm thấy trong [NMS] Thông tin yêu cầu DB
        ]
        self.SCHEMA_RAW = {
            'wallet_id': RAW_TYPE_INT64,
            'pan': RAW_TYPE_STR,
            'wallet_state_id': RAW_TYPE_INT64,
            'wallet_type_id': RAW_TYPE_INT64,
            'modified_date': RAW_TYPE_DATETIME,
            'created_date': RAW_TYPE_DATETIME,
            'active_time': RAW_TYPE_DATETIME,
            'currency_code': RAW_TYPE_STR,
            'customer_id': RAW_TYPE_DATETIME,
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "wallet_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "wallet_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKWalletBalance(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "balance", "mode": "NULLABLE", "type": "string"},
            {"name": "holding_balance", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "available_balance", "mode": "NULLABLE", "type": "bigint"},
            {"name": "date_modified", "mode": "NULLABLE", "type": "timestamp"},
        ]
        self.SCHEMA_RAW = {
            'wallet_id': RAW_TYPE_INT64,
            'balance': RAW_TYPE_STR,
            'holding_balance': RAW_TYPE_DATETIME,
            'available_balance': RAW_TYPE_INT64,
            'date_modified': RAW_TYPE_DATETIME,
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "wallet_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "wallet_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKWalletBalanceChange(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "balance_change_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "transaction_id", "mode": "NULLABLE", "type": "string"},
            {"name": "request_log_id", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_accounting_id", "mode": "NULLABLE", "type": "string"},
            {"name": "amount", "mode": "NULLABLE", "type": "bigint"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "direction", "mode": "NULLABLE", "type": "bigint"},
            {"name": "before_balance", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "after_balance", "mode": "NULLABLE", "type": "bigint"},
            {"name": "date_created", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "content", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "trans_type", "mode": "NULLABLE", "type": "bigint"},
            {"name": "before_balance_raw", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'balance_change_id': RAW_TYPE_INT64,
            'wallet_id': RAW_TYPE_INT64,
            'transaction_id': RAW_TYPE_STR,
            'request_log_id': RAW_TYPE_STR,
            'trans_accounting_id': RAW_TYPE_STR,
            'amount': RAW_TYPE_INT64,
            'currency_code': RAW_TYPE_STR,
            'direction': RAW_TYPE_INT64,
            'before_balance': RAW_TYPE_DATETIME,
            'after_balance': RAW_TYPE_INT64,
            'date_created': RAW_TYPE_DATETIME,
            'content': RAW_TYPE_INT64,
            'status': RAW_TYPE_DATETIME,
            'trans_type': RAW_TYPE_INT64,
            'before_balance_raw': RAW_TYPE_STR,
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "balance_change_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "balance_change_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKWalletState(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_state_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "locale_key", "mode": "NULLABLE", "type": "string"},
            {"name": "next_state", "mode": "NULLABLE", "type": "string"},
            {"name": "before_state", "mode": "NULLABLE", "type": "timestamp"},
        ]
        self.SCHEMA_RAW = {
            'wallet_state_id': RAW_TYPE_INT64,
            'name': RAW_TYPE_STR,
            'locale_key': RAW_TYPE_STR,
            'next_state': RAW_TYPE_STR,
            'before_state': RAW_TYPE_DATETIME,
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "wallet_state_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "wallet_state_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKWalletMaster(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "pan", "mode": "NULLABLE", "type": "string"},            
            {"name": "wallet_state_id", "mode": "NULLABLE", "type": "int"},
            {"name": "wallet_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "code", "mode": "NULLABLE", "type": "string"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "parent_wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "modified_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "business_wallet_type", "mode": "NULLABLE", "type": "int"},
            {"name": "client_id", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'wallet_id': RAW_TYPE_INT64,
            'pan': RAW_TYPE_STR,
            'wallet_state_id': RAW_TYPE_INT64,
            'wallet_type_id': RAW_TYPE_INT64,
            'name': RAW_TYPE_STR,
            'code': RAW_TYPE_STR,
            'currency_code': RAW_TYPE_STR,
            'parent_wallet_id': RAW_TYPE_INT64,
            'modified_date': RAW_TYPE_DATETIME,
            'created_date': RAW_TYPE_DATETIME,
            'business_wallet_type': RAW_TYPE_INT,
            'client_id': RAW_TYPE_STR
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "wallet_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "wallet_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKWalletType(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "prefix_code", "mode": "NULLABLE", "type": "string"},
            {"name": "type", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'wallet_type_id': RAW_TYPE_INT64,
            'name': RAW_TYPE_STR,
            'prefix_code': RAW_TYPE_STR,
            'type': RAW_TYPE_INT64,
            'status': RAW_TYPE_INT64
        }
        
        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "wallet_type_id", "type": "int"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "wallet_type_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKTransaction(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "transaction_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_type_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "partner_code", "mode": "NULLABLE", "type": "string"},
            {"name": "service_code", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_type", "mode": "NULLABLE", "type": "string"},
            {"name": "date_created", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "date_modified", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "date_expiried", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "date_finished", "mode": "NULLABLE", "type": "timestamp"},
        ]
        self.SCHEMA_RAW = {
            'transaction_id': RAW_TYPE_INT64,
            'currency_code': RAW_TYPE_STR,
            'trans_type_id': RAW_TYPE_INT64,
            'partner_code': RAW_TYPE_STR,
            'service_code': RAW_TYPE_STR,
            'trans_type': RAW_TYPE_STR,
            'date_created': RAW_TYPE_DATETIME,
            'date_modified': RAW_TYPE_DATETIME,
            'date_expiried': RAW_TYPE_DATETIME,
            'date_finished': RAW_TYPE_DATETIME,
        }
        
        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "transaction_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "transaction_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKWalletControlBalanceChange(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_control_balance_change_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "wallet_control_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "transaction_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "trans_type_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "amount", "mode": "NULLABLE", "type": "bigint"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "direction", "mode": "NULLABLE", "type": "bigint"},
            {"name": "before_balance", "mode": "NULLABLE", "type": "bigint"},
            {"name": "after_balance", "mode": "NULLABLE", "type": "bigint"},
            {"name": "date_created", "mode": "NULLABLE", "type": "timestamp"},
        ]
        self.SCHEMA_RAW = {
            'wallet_control_balance_change_id': RAW_TYPE_INT64,
            'wallet_control_id': RAW_TYPE_INT64,
            'transaction_id': RAW_TYPE_INT64,
            'trans_type_id': RAW_TYPE_INT64,
            'amount': RAW_TYPE_INT64,
            'currency_code': RAW_TYPE_STR,
            'direction': RAW_TYPE_INT64,
            'before_balance': RAW_TYPE_INT64,
            'after_balance': RAW_TYPE_INT64,
            'date_created': RAW_TYPE_DATETIME,
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "wallet_control_balance_change_id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "wallet_control_balance_change_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


class DLKControlAccount(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "code", "mode": "NULLABLE", "type": "string"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "wallet_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "status_code", "mode": "NULLABLE", "type": "bigint"},
            {"name": "bank_code", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
        ]
        self.SCHEMA_RAW = {
            'id': RAW_TYPE_INT64,
            'code': RAW_TYPE_STR,
            "name": RAW_TYPE_STR,
            'currency_code': RAW_TYPE_STR,
            'wallet_type_id': RAW_TYPE_INT64,
            'status_code': RAW_TYPE_INT64,
            'bank_code': RAW_TYPE_STR,
            'created_at': RAW_TYPE_DATETIME,
            'updated_at': RAW_TYPE_DATETIME,
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint"}
        ]
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''

_ALL = "all"
""" ALL table name in database """

DLK_WALLET = "wallet"
DLK_WALLET_BALANCE = "wallet_balance"
DLK_WALLET_BALANCE_CHANGE = "wallet_balance_change"
DLK_WALLET_STATE = "wallet_state"
DLK_WALLET_MASTER = "wallet_master"
DLK_WALLET_TYPE = "wallet_type"
DLK_TRANSACTION = "transaction"
DLK_WALLET_CONTROL_BALANCE_CHANGE = "wallet_control_balance_change"
DLK_CONTROL_ACCOUNT = "control_account"

W3_SYSTEM_ACCOUNTING_TABLE_SCHEMA = {
    DLK_WALLET: DLKWallet(DLK_WALLET),
    DLK_WALLET_BALANCE: DLKWalletBalance(DLK_WALLET_BALANCE),
    DLK_WALLET_BALANCE_CHANGE: DLKWalletBalanceChange(DLK_WALLET_BALANCE_CHANGE),
    DLK_WALLET_STATE: DLKWalletState(DLK_WALLET_STATE),
    DLK_WALLET_MASTER: DLKWalletMaster(DLK_WALLET_MASTER),
    DLK_WALLET_TYPE: DLKWalletType(DLK_WALLET_TYPE),
    DLK_TRANSACTION: DLKTransaction(DLK_TRANSACTION),
    DLK_WALLET_CONTROL_BALANCE_CHANGE: DLKWalletControlBalanceChange(DLK_WALLET_CONTROL_BALANCE_CHANGE),
    DLK_CONTROL_ACCOUNT: DLKControlAccount(DLK_CONTROL_ACCOUNT),
}

_ALL_DIM = [
    DLK_WALLET
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
