"""
Define table schema for staging
"""
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKPayment(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "request_id", "mode": "NULLABLE", "type": "string"},
            {"name": "channel_id", "mode": "NULLABLE", "type": "string"},
            {"name": "client_request_id", "mode": "NULLABLE", "type": "string"},
            {"name": "created_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "customer_account", "mode": "NULLABLE", "type": "string"},
            {"name": "customer_id", "mode": "NULLABLE", "type": "string"},
            {"name": "customer_type", "mode": "NULLABLE", "type": "string"},
            {"name": "msisdn", "mode": "NULLABLE", "type": "string"},
            {"name": "payment_id", "mode": "NULLABLE", "type": "string"},
            {"name": "retry", "mode": "NULLABLE", "type": "int"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "status_message", "mode": "NULLABLE", "type": "string"},
            {"name": "total_amount", "mode": "NULLABLE", "type": "decimal(19, 2)"},
            {"name": "total_commission", "mode": "NULLABLE", "type": "decimal(19, 2)"},
            {"name": "total_fee", "mode": "NULLABLE", "type": "decimal(19, 2)"},
            {"name": "total_revenue_shared", "mode": "NULLABLE", "type": "decimal(19, 2)"},
            {"name": "total_tax", "mode": "NULLABLE", "type": "decimal(19, 2)"},
            {"name": "trans_type", "mode": "NULLABLE", "type": "string"},
            {"name": "updated_date", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "user_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "total_tax_of_commission", "mode": "NULLABLE", "type": "decimal(19, 2)"},
            {"name": "api_code", "mode": "NULLABLE", "type": "string"},
            {"name": "order_id", "mode": "NULLABLE", "type": "string"},
            {"name": "from_role_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "from_tier", "mode": "NULLABLE", "type": "bigint"},
            {"name": "identify_type", "mode": "NULLABLE", "type": "string"},
            {"name": "response_code", "mode": "NULLABLE", "type": "string"},
            {"name": "client_id_token", "mode": "NULLABLE", "type": "string"},
            {"name": "email_token", "mode": "NULLABLE", "type": "string"},
            {"name": "from_name", "mode": "NULLABLE", "type": "string"},
            {"name": "to_name", "mode": "NULLABLE", "type": "string"},
        ]
        self.SCHEMA_RAW = {
            'REQUEST_ID': 'str',
            'CHANNEL_ID': 'str',
            'CLIENT_REQUEST_ID': 'str',
            'CREATED_DATE': 'datetime64[ns]',
            'CUSTOMER_ACCOUNT': 'str',
            'CUSTOMER_ID': 'str',
            'CUSTOMER_TYPE': 'str',
            'MSISDN': 'str',
            'PAYMENT_ID': 'str',
            'RETRY': 'int',
            'STATUS': 'str',
            'STATUS_MESSAGE': 'str',
            'TOTAL_AMOUNT': 'float64',
            'TOTAL_COMMISSION': 'float64',
            'TOTAL_FEE': 'float64',
            'TOTAL_REVENUE_SHARED': 'float64',
            'TOTAL_TAX': 'float64',
            'TRANS_TYPE': 'str',
            'UPDATED_DATE': 'datetime64[ns]',
            'USER_ID': 'int64',
            'TOTAL_TAX_OF_COMMISSION': 'float64',
            'API_CODE': 'str',
            'ORDER_ID': 'str',
            'FROM_ROLE_ID': 'int64',
            'FROM_TIER': 'int64',
            'IDENTITY_TYPE': 'str',
            'RESPONSE_CODE': 'str',
            'CLIENT_ID_TOKEN': 'str',
            'EMAIL_TOKEN': 'str',
            'FROM_NAME': 'str',
            'TO_NAME': 'str',
        }

        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "request_id", "type": "bigint"}
        ]
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "request_id",
            "JOIN": ""
        }
        self.WRAP_CHAR = ''


_ALL = "all"
""" ALL table name in database """

DLK_PAYMENT = "payment"

W3_CP_PAYMENT_BUSINESS_TABLE_SCHEMA = {
    DLK_PAYMENT: DLKPayment(DLK_PAYMENT),
}

_ALL_DIM = [
    DLK_PAYMENT
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
