"""
Define table schema for staging
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKTransactionUtility(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "transaction_id", "mode": "NULLABLE", "type": "string"},
            {"name": "request_detail_id", "mode": "NULLABLE", "type": "string"},
            {"name": "from_wallet_id", "mode": "NULLABLE", "type": "string"},
            {"name": "from_client_id", "mode": "NULLABLE", "type": "string"},
            {"name": "from_user_id", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "from_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "from_tier", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "to_wallet_id", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "to_client_id", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "to_user_id", "mode": "NULLABLE", "type": "string"},
            {"name": "to_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "to_tier", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "partner_code", "mode": "NULLABLE", "type": "bigint"},
            {"name": "service_code", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "transaction_state", "mode": "NULLABLE", "type": "bigint"},

            {"name": "payment_source", "mode": "NULLABLE", "type": "string"},
            {"name": "pricing_rule_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "bigint"},
            {"name": "amount", "mode": "NULLABLE", "type": "bigint"},
            {"name": "fee", "mode": "NULLABLE", "type": "bit"},
            {"name": "discount", "mode": "NULLABLE", "type": "string"},
            {"name": "commission", "mode": "NULLABLE", "type": "bigint"},
            {"name": "tax", "mode": "NULLABLE", "type": "string"},
            {"name": "revenue_shared", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_date", "mode": "NULLABLE", "type": "bigint"},
            {"name": "modified_date", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "modified_by", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_type", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'id': 'str',
            'order_id': 'str',
            'trans_type': 'str',
            'transfer_type': 'str',
            'amount': 'float64',
            'pricing_rule_id': 'int64',
            'discount': 'float64',
            'commission': 'float64',
            'fee': 'float64',
            'access_channel': 'str',
            'version': 'int64',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',

            'carrier_tel': 'str',
            'carrier_wallet_id': 'int64',
            'carrier_role_id': 'int64',
            'carrier_tier_id': 'int64',
            'is_deleted': 'bool',
            'request_id': 'str',
            'sub_transfer_type': 'int64',
            'payment_source': 'str',
            'sender_cus_id': 'int64',
            'sender_role_id': 'int64',
            'sender_tier_id': 'int64',
            'sender_wallet_id': 'int64',
            'sender_name': 'str',
            'sender_third_party_info': 'str',
            'receiver_account_type': 'str',
            'receiver_partner_code': 'str',
            'receiver_partner_name': 'str',
            'receiver_account_number': 'str',
            'receiver_name': 'str',
            'receiver_fspid': 'str',
            'currency_code': 'str',
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


class DLKClientRequest(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "client_request_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "request_id", "mode": "NULLABLE", "type": "string"},
            {"name": "request_type", "mode": "NULLABLE", "type": "string"},
            {"name": "refund_request_id", "mode": "NULLABLE", "type": "string"},
            {"name": "transfer_order_id", "mode": "NULLABLE", "type": "string"},
            {"name": "number_accounting", "mode": "NULLABLE", "type": "int"},
            {"name": "is_include_refund_fee", "mode": "NULLABLE", "type": "bit"},
            {"name": "is_allow_partial_refund", "mode": "NULLABLE", "type": "bit"},
            {"name": "partner_service_code", "mode": "NULLABLE", "type": "string"},
            {"name": "amount", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "fee", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "discount", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "tax", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "commission", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "refund_reason", "mode": "NULLABLE", "type": "string"},

            {"name": "status_code", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "string"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "string"},
            {"name": "version", "mode": "NULLABLE", "type": "bigint"},
            {"name": "is_deleted", "mode": "NULLABLE", "type": "bit"},
            {"name": "transaction_id", "mode": "NULLABLE", "type": "string"},
            {"name": "error_code", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'request_id': 'str',
            'request_type': 'str',
            'refund_request_id': 'str',
            'transfer_order_id': 'str',
            'number_accounting': 'int64',
            'is_include_refund_fee': 'bool',
            'is_allow_partial_refund': 'bool',
            'partner_service_code': 'str',
            'amount': 'float64',
            'fee': 'float64',
            'discount': 'float64',
            'tax': 'float64',
            'commission': 'float64',
            'refund_reason': 'str',

            'status_code': 'str',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'version': 'int64',
            'is_deleted': 'bool',
            'transaction_id': 'str',
            'error_code': 'str',
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


class DLKRequestDetail(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "request_detail_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "client_request_id", "mode": "NULLABLE", "type": "string"},
            {"name": "transaction_id", "mode": "NULLABLE", "type": "string"},
            {"name": "request_type", "mode": "NULLABLE", "type": "string"},
            {"name": "group_transtype", "mode": "NULLABLE", "type": "string"},
            {"name": "partner_code", "mode": "NULLABLE", "type": "string"},
            {"name": "service_code", "mode": "NULLABLE", "type": "string"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},
            {"name": "access_channel", "mode": "NULLABLE", "type": "string"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
            {"name": "bank_code", "mode": "NULLABLE", "type": "string"},
            {"name": "bank_name", "mode": "NULLABLE", "type": "string"},
            {"name": "invoice_number", "mode": "NULLABLE", "type": "string"},
            {"name": "payment_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "from_wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "to_wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "amount", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "transaction_time", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "version", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "string"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "string"},
            {"name": "status_code", "mode": "NULLABLE", "type": "string"},
            {"name": "request_status", "mode": "NULLABLE", "type": "string"},
            {"name": "is_deleted", "mode": "NULLABLE", "type": "bit"},
            {"name": "action_type", "mode": "NULLABLE", "type": "string"},
            {"name": "error_code", "mode": "NULLABLE", "type": "string"},
            {"name": "order_id", "mode": "NULLABLE", "type": "string"},
            {"name": "from_control_account_id", "mode": "NULLABLE", "type": "string"},
            {"name": "to_control_account_id", "mode": "NULLABLE", "type": "string"},
            {"name": "to_tier", "mode": "NULLABLE", "type": "bigint"},
            {"name": "to_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "adjust_type", "mode": "NULLABLE", "type": "string"},
            {"name": "promulgate_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "evidence_paper_type", "mode": "NULLABLE", "type": "string"},
            {"name": "evidence_paper_no", "mode": "NULLABLE", "type": "string"},
            {"name": "evidence_paper_no", "mode": "NULLABLE", "type": "string"},
            {"name": "control_account_code", "mode": "NULLABLE", "type": "string"},
            {"name": "control_account_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "internal_transfer_config", "mode": "NULLABLE", "type": "bit"},
            {"name": "partner_service_code", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type_code", "mode": "NULLABLE", "type": "string"},
            {"name": "from_tier", "mode": "NULLABLE", "type": "bigint"},
            {"name": "from_role_id", "mode": "NULLABLE", "type": "bigint"},
        ]
        self.SCHEMA_RAW = {
            'id': 'int64',
            'request_id': 'str',
            'request_type': 'str',
            'refund_request_id': 'str',
            'transfer_order_id': 'str',
            'number_accounting': 'int64',
            'is_include_refund_fee': 'bool',
            'is_allow_partial_refund': 'bool',
            'partner_service_code': 'str',
            'amount': 'float64',
            'fee': 'float64',
            'discount': 'float64',
            'tax': 'float64',
            'commission': 'float64',
            'refund_reason': 'str',

            'status_code': 'str',
            'created_at': 'datetime64[ns]',
            'created_by': 'int64',
            'updated_at': 'datetime64[ns]',
            'updated_by': 'int64',
            'version': 'int64',
            'is_deleted': 'bool',
            'transaction_id': 'str',
            'error_code': 'str',
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

DLK_TRANSACTION_UTILITY = "transaction_utility"
DLK_CLIENT_REQUEST = "client_request"
DLK_REQUEST_DETAIL = "request_detail"

W3_UTILITY_MANAGEMENT_TABLE_SCHEMA = {
    DLK_TRANSACTION_UTILITY: DLKTransactionUtility(DLK_TRANSACTION_UTILITY),
    DLK_CLIENT_REQUEST: DLKClientRequest(DLK_CLIENT_REQUEST),
    DLK_REQUEST_DETAIL: DLKRequestDetail(DLK_REQUEST_DETAIL),
}

_ALL_DIM = [
    DLK_TRANSACTION_UTILITY,
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
