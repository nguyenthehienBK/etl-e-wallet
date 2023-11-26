import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook


def get_hdfs_path(
        table_name: str = None,
        hdfs_conn_id: str = None,
        layer: str = None,
        bucket: str = None,
        business_day: str = "19700101",
) -> str:
    conn = BaseHook.get_connection(hdfs_conn_id)
    host = conn.host
    port = str(conn.port)
    # host = 'host_test'
    # port = 'post_test'
    if table_name is None:
        return ""
    if layer == "BRONZE":
        return f"hdfs://{host}:{port}/{bucket}/{layer}/{table_name}/{business_day}/"
    else:
        return f"hdfs://{host}:{port}/{bucket}/{layer}/{table_name}/"


def get_host_port(hdfs_conn_id: str = None):
    conn = BaseHook.get_connection(hdfs_conn_id)
    host = conn.host
    port = str(conn.port)
    return host, port


def get_content_from_sql_path(sql_path: str) -> str:
    data = ''
    try:
        with open(sql_path, 'r') as file:
            data = file.read()
    except:
        print(f'Loi doc file sql {sql_path}')
    return data


from utils.database.db_data_type import UpsertType

PAGING = 100000
MAXIMUM_FILE_LOAD_GCS2BQ = 10000


def get_etl_time():
    etl_time = Variable.get("kv_etl_time", default_var={}, deserialize_json=True)
    from_date = etl_time.get("from")
    to_date = etl_time.get("to")
    if not from_date:
        from_date = (datetime.datetime.now() - relativedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
    if not to_date:
        to_date = (datetime.datetime.now() - relativedelta(days=1)).strftime("%Y-%m-%d")
    return from_date, to_date


"""
File path:
- Dim: dimensions_table/Branch/*.parquet AND dimensions_table/Branch/2019-08-26/*.parquet
- Facts: facts_table/2019-08-26/share_prod_105/*.parquet
fact_path = "FACTS_TABLE/extract_date_str/SHARE_PROD_FORMAT/table_name.parquet"
dimension_path = "DIMENSIONS_TABLE/table_name/server_key__table_name__paging__file_idx.parquet"

"""

# FACTS_TABLE = 'facts_table'
# DIMENSIONS_TABLE = 'dimensions_table'
FIXED_TABLE = "fixed_table"
CREATE_NEVER = "CREATE_NEVER"
CREATE_IF_NEEDED = "CREATE_IF_NEEDED"
WRITE_TRUNCATE = "WRITE_TRUNCATE"
WRITE_APPEND = "WRITE_APPEND"
WRITE_EMPTY = "WRITE_EMPTY"

# BLOB_NAME_DIM = "{}/{}/{}.parquet"
# BLOB_NAME_FACTS = "{}/{}/{}/{}.parquet"
# SHARE_PROD_FORMAT = "share_prod_"


"""
File path:
- Dim: dimensions_table/kv_mssql/2019-08-26/shard_105/Branch/Branch_*.parquet
- Facts: facts_table/kv_mssql/2019-08-26/shard_105/Invoice/Invoice_*.parquet
"""


def get_partition_column_expr(table, alias_table=None):
    if hasattr(table, "TIME_PARTITIONING") and table.TIME_PARTITIONING:
        partition = table.TIME_PARTITIONING
        field_type = partition["type"].upper()
        if alias_table:
            field = f'{alias_table}.`{partition["field"]}`'
        else:
            field = f'`{partition["field"]}`'

        if field_type == UpsertType.DAY:
            partition_colum_expr = f"date({field})"
        elif field_type == UpsertType.NONE:
            partition_colum_expr = field
        else:
            partition_colum_expr = f"{field_type}({field})"

        return partition_colum_expr
    else:
        return None


class ExtractSQL:
    SQL_TEMPLATE_DIM = "dags/sql/template/extract_sql_template_dim.sql"
    SQL_TEMPLATE_FACT = "dags/sql/template/extract_sql_template_facts.sql"
    EQUAL_FORMAT = "WHERE {} = '{}'"
    BETWEEN_FORMAT = "WHERE {} BETWEEN '{}' AND '{}'"


def get_sql_param(tbl):
    where_condition = ""
    timestamp = ""
    join = ""
    order_by = ""
    ext_columns = ""
    timestamp_key = ""
    sql_source_file = False

    table_name = tbl.TABLE_NAME
    prefix = table_name
    timestamp = tbl.EXTRACT["TIMESTAMP"]
    join = tbl.EXTRACT["JOIN"]
    order_by = tbl.EXTRACT["ORDER_BY"]
    timestamp_key = tbl.EXTRACT["TIMESTAMP_KEY"]
    ls_columns = [s["name"] for s in tbl.SCHEMA]
    columns = '`' + ls_columns[0] + '` AS ' + ls_columns[0]
    for col in ls_columns[1:]:
        columns = columns + ',`' + col + '` AS ' + col + '\n'

    sql_val = {
        "columns": columns,
        "table_name": prefix,
        "where_condition": where_condition,
        "timestamp": timestamp,
        "join": join,
        "order_by": order_by,
    }
    if tbl.TABLE_TYPE == "DIM":
        sql_file = ExtractSQL.SQL_TEMPLATE_DIM
    else:
        sql_file = ExtractSQL.SQL_TEMPLATE_FACT
    query = get_content_from_sql_path(sql_file)

    return {
        "file": sql_file,
        "query": query,
        "timestamp_key": timestamp_key,
        "params": sql_val,
        "sql_source_file": sql_source_file
    }
