"""
Define table schema for staging
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.type.data_type import *
from utils.lakehouse.table_utils import get_content_from_sql_path


class MartWallet(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "pan", "mode": "NULLABLE", "type": "string"},
            {"name": "wallet_state_id", "mode": "NULLABLE", "type": "int"},
            {"name": "wallet_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "modified_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "active_time", "mode": "NULLABLE", "type": "datetime"}, 
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "customer_id", "mode": "NULLABLE", "type": "string"},
        ]

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


class MartWalletBalance(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "balance", "mode": "NULLABLE", "type": "string"},
            {"name": "holding_balance", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "available_balance", "mode": "NULLABLE", "type": "bigint"},
            {"name": "date_modified", "mode": "NULLABLE", "type": "timestamp"},
        ]

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


class MartWalletBalanceChange(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "balance_change_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "transaction_id", "mode": "NULLABLE", "type": "string"},
            {"name": "request_log_id", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_accounting_id", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_type_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "amount", "mode": "NULLABLE", "type": "bigint"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "date_created", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "content", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "trans_type", "mode": "NULLABLE", "type": "bigint"},
        ]

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


class MartWalletState(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_state_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "locale_key", "mode": "NULLABLE", "type": "string"},
            {"name": "next_state", "mode": "NULLABLE", "type": "string"},
        ]

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


class MartWalletMaster(DaoDim, BaseModel):
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
        ]

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


class MartWalletType(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "wallet_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "name", "mode": "NULLABLE", "type": "string"},
            {"name": "prefix_code", "mode": "NULLABLE", "type": "string"},
            {"name": "type", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "bigint"},
        ]
        
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


class MartTransaction(DaoDim, BaseModel):
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


class MartWalletControlBalanceChange(DaoDim, BaseModel):
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


class MartControlAccount(DaoDim, BaseModel):
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

class MartPartnerServices(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA =[
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "partner_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "service_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "priority_partner_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "sub_partner_service_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "partner_wallet_id", "mode": "NULLABLE", "type": "bigint"},
        ]
        
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


class MartPartnerInfos(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "partner_code", "mode": "NULLABLE", "type": "string"},
            {"name": "partner_name", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type_code", "mode": "NULLABLE", "type": "string"},
            {"name": "is_intermediary", "mode": "NULLABLE", "type": "bit"},
            {"name": "tax_code", "mode": "NULLABLE", "type": "string"},
            {"name": "business_license", "mode": "NULLABLE", "type": "string"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
        ]
        
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

class MartRoleTypes(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
            {"name": "parent_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "role_type_code", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type_name", "mode": "NULLABLE", "type": "string"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "system_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "is_partner", "mode": "NULLABLE", "type": "bool"},
            {"name": "is_default", "mode": "NULLABLE", "type": "bool"},
        ]

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

class MartUserRoleTypeRef(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "user_id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "role_type_id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "tier_id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "status", "mode": "NULLABLE", "type": "varchar(10)"},
            {"name": "phone_number_otp", "mode": "NULLABLE", "type": "varchar(20)"},
            {"name": "pin", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "password", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "count_invalid_otp", "mode": "NULLABLE", "type": "int(11)"},
            {"name": "count_invalid_pin", "mode": "NULLABLE", "type": "int(11)"},
            {"name": "created_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "client_id", "mode": "NULLABLE", "type": "varchar(100)"},
            {"name": "last_ip_address", "mode": "NULLABLE", "type": "varchar(100)"},
            {"name": "count_invalid_pw", "mode": "NULLABLE", "type": "int(11)"},
            {"name": "last_login_device_id", "mode": "NULLABLE", "type": "bigint(20)"},
        ]
        
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint(20)"}
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

class MartUsers(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "first_name", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "last_name", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "username", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "email", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "phone_number", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "birthday", "mode": "NULLABLE", "type": "date"},
            {"name": "country", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "image_url", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "status", "mode": "NULLABLE", "type": "varchar(10)"},
            {"name": "lang", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "sex", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "time_zone", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "reset_date", "mode": "NULLABLE", "type": "datetime"},
            {"name": "reset_key", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "activation_key", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "created_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint(20)"}
        ]
        
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint(20)"}
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

class MartUserWallets(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "role_type_ref_id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "wallet_id", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "created_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint(20)"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "datetime"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint(20)"},
        ]
        
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint(20)"}
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

MART_WALLET = "wallet"
MART_WALLET_BALANCE = "wallet_balance"
MART_WALLET_BALANCE_CHANGE = "wallet_balance_change"
MART_WALLET_STATE = "wallet_state"
MART_WALLET_MASTER = "wallet_master"
MART_WALLET_TYPE = "wallet_type"
MART_TRANSACTION = "transaction"
MART_WALLET_CONTROL_BALANCE_CHANGE = "wallet_control_balance_change"
MART_CONTROL_ACCOUNT = "control_account"
MART_PARTNER_SERVICES = "partner_services"
MART_PARTNER_INFOS = "partner_infos"
MART_ROLE_TYPES = "roles_types"
MART_USER_ROLE_TYPE_REF = "user_role_type_ref"
MART_USERS = "users"
MART_USER_WALLETS = "user_wallets"

W3_INTERNAL_REPORTING_TABLE_SCHEMA = {
    MART_WALLET: MartWallet(MART_WALLET),
    MART_WALLET_BALANCE: MartWalletBalance(MART_WALLET_BALANCE),
    MART_WALLET_BALANCE_CHANGE: MartWalletBalanceChange(MART_WALLET_BALANCE_CHANGE),
    MART_WALLET_STATE: MartWalletState(MART_WALLET_STATE),
    MART_WALLET_MASTER: MartWalletMaster(MART_WALLET_MASTER),
    MART_WALLET_TYPE: MartWalletType(MART_WALLET_TYPE),
    MART_TRANSACTION: MartTransaction(MART_TRANSACTION),
    MART_WALLET_CONTROL_BALANCE_CHANGE: MartWalletControlBalanceChange(MART_WALLET_CONTROL_BALANCE_CHANGE),
    MART_CONTROL_ACCOUNT: MartControlAccount(MART_CONTROL_ACCOUNT),
    MART_PARTNER_SERVICES: MartPartnerServices(MART_PARTNER_SERVICES),
    MART_PARTNER_INFOS: MartPartnerInfos(MART_PARTNER_INFOS),
    MART_ROLE_TYPES: MartRoleTypes(MART_ROLE_TYPES),
    MART_USER_ROLE_TYPE_REF: MartUserRoleTypeRef(MART_USER_ROLE_TYPE_REF),
    MART_USERS: MartUsers(MART_USERS),
    MART_USER_WALLETS: MartUserWallets(MART_USER_WALLETS)
}

_ALL_DIM = [
    MART_WALLET
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
