"""
Define table schema for staging
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKTransferOrder(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "string"},
            {"name": "order_id", "mode": "NULLABLE", "type": "string"},
            {"name": "trans_type", "mode": "NULLABLE", "type": "string"},
            {"name": "transfer_type", "mode": "NULLABLE", "type": "string"},
            {"name": "amount", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "pricing_rule_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "discount", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "commission", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "fee", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "access_channel", "mode": "NULLABLE", "type": "string"},
            {"name": "version", "mode": "NULLABLE", "type": "bigint"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},

            {"name": "carrier_tel", "mode": "NULLABLE", "type": "string"},
            {"name": "carrier_wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "carrier_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "carrier_tier_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "is_deleted", "mode": "NULLABLE", "type": "bit"},
            {"name": "request_id", "mode": "NULLABLE", "type": "string"},
            {"name": "sub_transfer_type", "mode": "NULLABLE", "type": "bigint"},
            {"name": "payment_source", "mode": "NULLABLE", "type": "string"},
            {"name": "sender_cus_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "sender_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "sender_tier_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "sender_wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "sender_name", "mode": "NULLABLE", "type": "string"},
            {"name": "sender_third_party_info", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_account_type", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_partner_code", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_partner_name", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_account_number", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_name", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_fspid", "mode": "NULLABLE", "type": "string"},
            {"name": "currency_code", "mode": "NULLABLE", "type": "string"},

            {"name": "tax", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "revenue_shared", "mode": "NULLABLE", "type": "decimal(15,2)"},
            {"name": "refer_trans_id", "mode": "NULLABLE", "type": "string"},
            {"name": "commission_on_level", "mode": "NULLABLE", "type": "string"},
            {"name": "tax_on_level", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_wallet_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "receiver_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "receiver_tier_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "refer_order_id", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_tel", "mode": "NULLABLE", "type": "string"},
            {"name": "status_code", "mode": "NULLABLE", "type": "string"},
            {"name": "secret_code", "mode": "NULLABLE", "type": "string"},
            {"name": "action_type", "mode": "NULLABLE", "type": "string"},
            {"name": "error_code", "mode": "NULLABLE", "type": "string"},
            {"name": "service_code", "mode": "NULLABLE", "type": "string"},
            {"name": "refer_receiver_tel", "mode": "NULLABLE", "type": "string"},
            {"name": "partner_code", "mode": "NULLABLE", "type": "string"},
            {"name": "partner_service_code", "mode": "NULLABLE", "type": "string"},
            {"name": "refer_secret_code", "mode": "NULLABLE", "type": "string"},
            {"name": "third_party_payload", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type_code", "mode": "NULLABLE", "type": "string"},
            {"name": "carrier_client_id", "mode": "NULLABLE", "type": "string"},
            {"name": "sender_client_id", "mode": "NULLABLE", "type": "string"},
            {"name": "receiver_client_id", "mode": "NULLABLE", "type": "string"},
            {"name": "fsp_id", "mode": "NULLABLE", "type": "string"},
            {"name": "refund_status", "mode": "NULLABLE", "type": "string"},
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

            'tax': 'float64',
            'revenue_shared': 'float64',
            'refer_trans_id': 'str',
            'commission_on_level': 'str',
            'tax_on_level': 'str',
            'receiver_wallet_id': 'int64',
            'receiver_role_id': 'int64',
            'receiver_tier_id': 'int64',
            'refer_order_id': 'str',
            'receiver_tel': 'str',
            'status_code': 'str',
            'secret_code': 'str',
            'action_type': 'str',
            'error_code': 'str',
            'service_code': 'str',
            'refer_receiver_tel': 'str',
            'partner_code': 'str',
            'partner_service_code': 'str',
            'refer_secret_code': 'str',
            'third_party_payload': 'str',
            'role_type_code': 'str',
            'carrier_client_id': 'str',
            'sender_client_id': 'str',
            'receiver_client_id': 'str',
            'fsp_id': 'str',
            'refund_status': 'str',
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


class DLKRefundRequest(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
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


class DLKStockRequest(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "request_id", "mode": "NULLABLE", "type": "string"},
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

DLK_TRANSFER_ORDER = "transfer_order"
DLK_REFUND_REQUEST = "refund_request"
DLK_STOCK_REQUEST = "stock_request"

W3_TRANSFER_BUSINESS_TABLE_SCHEMA = {
    DLK_TRANSFER_ORDER: DLKTransferOrder(DLK_TRANSFER_ORDER),
    DLK_REFUND_REQUEST: DLKRefundRequest(DLK_REFUND_REQUEST),
    DLK_STOCK_REQUEST: DLKStockRequest(DLK_STOCK_REQUEST),
}

_ALL_DIM = [
    DLK_TRANSFER_ORDER,
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
