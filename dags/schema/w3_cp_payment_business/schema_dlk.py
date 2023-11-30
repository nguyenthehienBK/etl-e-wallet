"""
Define table schema for staging
"""
from airflow.models import Variable
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.database.db_data_type import UpsertType
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
            'request_id': 'str',
            'channel_id': 'str',
            'client_request_id': 'str',
            'created_date': 'datetime64[ns]',
            'customer_account': 'str',
            'customer_id': 'str',
            'customer_type': 'str',
            'msisdn': 'str',
            'payment_id': 'str',
            'retry': 'int64',
            'status': 'str',
            'status_message': 'str',
            'total_amount': 'float',
            'total_commission': 'float',
            'total_fee': 'float',
            'total_revenue_shared': 'float',
            'total_tax': 'float',
            'trans_type': 'str',
            'updated_date': 'datetime64[ns]',
            'user_id': 'int64',
            'total_tax_of_commission': 'float',
            'api_code': 'str',
            'order_id': 'str',
            'from_role_id': 'int64',
            'from_tier': 'int64',
            'identify_type': 'str',
            'response_code': 'str',
            'client_id_token': 'str',
            'email_token': 'str',
            'from_name': 'str',
            'to_name': 'str',
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
