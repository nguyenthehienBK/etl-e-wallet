"""
Define table schema for staging
File dags/schema/kv_mssql/schema_dlk.py
"""
from airflow.models import Variable
from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE
from utils.database.db_data_type import UpsertType
from utils.lakehouse.table_utils import get_content_from_sql_path


class DLKBranch(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA = [
            {"name": "ServerKey", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "Id", "type": "BIGINT", "mode": "NULLABLE"},
            {"name": "Name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Province", "type": "STRING", "mode": "NULLABLE"},
            {"name": "WardName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "District", "type": "STRING", "mode": "NULLABLE"},
            {"name": "IsActive", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "CreatedDate", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "ModifiedDate", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "RetailerId", "type": "BIGINT", "mode": "NULLABLE"},
            {"name": "PharmacySyncStatus", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "PharmacyConnectCode", "type": "STRING", "mode": "NULLABLE"},
            {"name": "GmbStatus", "type": "BIGINT", "mode": "NULLABLE"},
            {"name": "GmbLocationName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LimitAccess", "type": "BOOLEAN", "mode": "NULLABLE"}
        ]
        # self.COLUMNS_SCHEMA = self.DEFAULT_COLUMNS + self.SCHEMA
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = True
        self.KEY_COLUMNS = [
            {"name": "Id", "type": "BIGINT"},
            {"name": "ServerKey", "mode": "NULLABLE", "type": "BIGINT"},
        ]
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.EXTRACT = {
            "TIMESTAMP": "",
            "SQL": ExtractSQL.SQL_TEMPLATE_DIM,
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "Id",
            "JOIN": ""
        }


class DLKInvoice(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SQL_SOURCE_FILE = True
        self.SCHEMA = [
            {"name": "ServerKey", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "Id", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "CustomerId", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "PurchaseDate", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "Code", "mode": "NULLABLE", "type": "STRING"},
            {"name": "PaymentType", "mode": "NULLABLE", "type": "STRING"},
            {"name": "BranchId", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "Status", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "ModifiedDate", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "CreatedDate", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "Total", "mode": "NULLABLE", "type": "DECIMAL(38, 9)"},
            {"name": "TotalPayment", "mode": "NULLABLE", "type": "DECIMAL(38, 9)"},
            {"name": "UsingCod", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "SaleChannelId", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "RetailerId", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "Uuid", "mode": "NULLABLE", "type": "STRING"},
            {"name": "CreatedBy", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "ModifiedBy", "mode": "NULLABLE", "type": "BIGINT"}
        ]

        self.TIME_PARTITIONING = {
                "type": UpsertType.DAY,
                "field": "CreatedDate"
            }

        # For extract data
        self._TIMESTAMP = "CAST(CASE WHEN [Order].ModifiedDate is NULL THEN [Order].CreatedDate ELSE [Order].ModifiedDate END as Date)"
        self.EXTRACT = {
            "TIMESTAMP": self._TIMESTAMP,
            "SQL": ExtractSQL.SQL_TEMPLATE_FACT,
            "TIMESTAMP_KEY": "timestamp",
            "ORDER_BY": self._TIMESTAMP + ", Id",
            "JOIN": ""
        }
        self.COLUMNS_SCHEMA = self.SCHEMA
        self.IS_WRITE_TRUNCATE = False
        self.KEY_COLUMNS = [
            {"name": "Id", "type": "BIGINT"},
            {"name": "ServerKey", "mode": "NULLABLE", "type": "BIGINT"},
        ]
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = FACT_TABLE_TYPE


class ExtractSQL:
    SQL_TEMPLATE_DIM = "dags/sql/template/extract_sql_template_dim.sql"
    SQL_TEMPLATE_FACT = "dags/sql/template/extract_sql_template_facts.sql"
    EQUAL_FORMAT = "WHERE {} = '{}'"
    BETWEEN_FORMAT = "WHERE {} BETWEEN '{}' AND '{}'"


_ALL = "all"
""" ALL table name in database """

DLK_BRANCH = "Gen11"
DLK_INVOICE = "Gen2"


GENERIC_TABLE_SCHEMA = {
    DLK_BRANCH: DLKBranch(DLK_BRANCH),
    DLK_INVOICE: DLKInvoice(DLK_INVOICE),
}


_ALL_DIM = [
    DLK_BRANCH
]


_ALL_FACT = [
    DLK_INVOICE
]


_ALL_TABLE = _ALL_DIM + _ALL_FACT


def get_table_info(table_name, only_schema=False):
    tbl = _TABLE_SCHEMA.get(table_name)
    if only_schema and tbl:
        return tbl.SCHEMA
    return tbl


def get_columns(table_name, prefix=""):
    schemas = get_table_info(table_name=table_name, only_schema=True)
    if not schemas:
        return []
    if prefix:
        return ["{}.{}".format(prefix, s["name"]) for s in schemas]
    return [s["name"] for s in schemas]


def valid_all_tables(ls_tbl):
    if ls_tbl == _ALL:
        return _ALL_TABLE
    return [tbl for tbl in ls_tbl if tbl in _ALL_TABLE]


def valid_tables(ls_tbl, include_schema=False, except_table=None, include_master=False):
    results = []
    if ls_tbl == _ALL:
        if include_master:
            ls_tbl = _ALL_TABLE
        else:
            ls_tbl = _ALL_DIM + _ALL_FACT

    for tbl in ls_tbl:
        tbl_name = None
        is_fact = False
        if except_table and tbl in except_table:
            continue
        if tbl in _ALL_DIM:
            tbl_name = tbl
            is_fact = False
        elif tbl in _ALL_FACT:
            tbl_name = tbl
            is_fact = True

        if tbl_name:
            tb_dict = {
                "name": tbl_name,
                "is_fact": is_fact
            }
            if include_schema:
                schema = get_table_info(table_name=tbl_name, only_schema=True)
                tb_dict["schema"] = schema
            results.append(tb_dict)
    return results


def get_sql(table_name, is_fact=False, etl_from=None, etl_to=None):
    where_condition = ""
    timestamp = ""
    join = ""
    order_by = ""
    ext_columns = ""
    timestamp_key = ""
    retailer_condition = ""
    sql_source_file = False

    tbl = get_table_info(table_name=table_name)
    if tbl:
        ext_config = tbl.EXTRACT
        timestamp = ext_config["TIMESTAMP"]
        timestamp_key = ext_config["TIMESTAMP_KEY"]
        join = ext_config["JOIN"]
        order_by = ext_config["ORDER_BY"]
        if timestamp_key:
            ext_columns = ",{} as {}".format(timestamp, timestamp_key)
        if hasattr(tbl, 'SQL_SOURCE_FILE'):
            sql_source_file = tbl.SQL_SOURCE_FILE
    if is_fact:
        if etl_from == etl_to:
            where_condition = ExtractSQL.EQUAL_FORMAT.format(timestamp, etl_from)
        else:
            where_condition = ExtractSQL.BETWEEN_FORMAT.format(timestamp, etl_from, etl_to)

    prefix = table_name

    ls_columns = get_columns(table_name=table_name, prefix=prefix)
    columns = ''
    for col in ls_columns:
        columns = columns + col + '\n'
    # Phân biệt Retailer test: có dạng testzxx, ...
    # TODO remove retailer case from automatic generate sql to specific sql file

    sql_val = {
        "columns": columns,
        "table_name": prefix,
        "where_condition": where_condition,
        "timestamp": timestamp,
        "join": join,
        "order_by": order_by,
        "retailer_condition": retailer_condition,
        "ext_columns": ext_columns
    }
    sql_file = ExtractSQL.SQL_TEMPLATE
    query = get_content_from_sql_path(sql_file)

    return {
        "file": sql_file,
        "query": query,
        "timestamp_key": timestamp_key,
        "params": sql_val,
        "sql_source_file": sql_source_file
    }


def is_fact_table(table_name):
    """
    table is fact or not
    """
    return table_name in _ALL_FACT


