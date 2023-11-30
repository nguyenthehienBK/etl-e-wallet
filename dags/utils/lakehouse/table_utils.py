import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from utils.database.db_data_type import UpsertType

PAGING = 100000
MAXIMUM_FILE_LOAD_GCS2BQ = 10000


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


def get_pure_type(col_type: str):
    if 'decimal' in col_type:
        return 'decimal'
    else:
        return col_type


def is_cast_column(col_type: str):
    if get_pure_type(col_type) in ('bigint', 'decimal'):
        return True
    else:
        return False


def get_merge_query_dwh(tbl):
    # create table statement
    create_table_sql = '`' + tbl.SCHEMA[0]["name"] + '`' + ' ' + tbl.SCHEMA[0]["type"]
    for schema in tbl.SCHEMA[1:]:
        create_table_sql = create_table_sql + '\n,`' + schema["name"] + '`' + ' ' + schema["type"]
    # print(create_table_sql)

    # select statement
    select_sql = "SELECT "
    if is_cast_column(tbl.SCHEMA[0]["type"]):
        select_sql = select_sql + get_pure_type(tbl.SCHEMA[0]["type"]) + '(' + tbl.SCHEMA[0]["name"] + ') AS ' + tbl.SCHEMA[0][
            "name"]
    else:
        select_sql = select_sql + tbl.SCHEMA[0]["name"] + ' AS ' + tbl.SCHEMA[0]["name"]
    for schema in tbl.SCHEMA[1:]:
        if is_cast_column(schema["type"]):
            select_sql = select_sql + '\n,' + get_pure_type(schema["type"]) + '(' + schema["name"] + ') AS ' + schema[
                "name"]
        else:
            select_sql = select_sql + '\n,' + schema["name"] + ' AS ' + schema["name"]

    # on match statement
    match_conditions = ' '
    match_conditions = match_conditions + 't.' + tbl.KEY_COLUMNS[0]["name"] + '= s.' + tbl.KEY_COLUMNS[0]["name"]
    for schema in tbl.KEY_COLUMNS[1:]:
        match_conditions = match_conditions + ' AND t.' + schema["name"] + '= s.' + schema["name"]

    # merge statement
    merge_clause = ''
    merge_clause = merge_clause + 't.' + tbl.SCHEMA[0]["name"] + ' = s.' + tbl.SCHEMA[0]["name"]
    for schema in tbl.SCHEMA[1:]:
        merge_clause = merge_clause + '\n,t.' + schema["name"] + ' = s.' + schema["name"]

    return {
        "create_table_sql": create_table_sql,
        "select_sql": select_sql,
        "match_conditions": match_conditions,
        "merge_clause": merge_clause
    }


def get_content_from_sql_path(sql_path: str) -> str:
    data = ''
    try:
        with open(sql_path, 'r') as file:
            data = file.read()
    except:
        print(f'Loi doc file sql {sql_path}')
    return data


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


FIXED_TABLE = "fixed_table"
CREATE_NEVER = "CREATE_NEVER"
CREATE_IF_NEEDED = "CREATE_IF_NEEDED"
WRITE_TRUNCATE = "WRITE_TRUNCATE"
WRITE_APPEND = "WRITE_APPEND"
WRITE_EMPTY = "WRITE_EMPTY"


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
    wrap_char = tbl.WRAP_CHAR
    ls_columns = [s["name"] for s in tbl.SCHEMA]
    # columns = '`' + ls_columns[0] + '` AS ' + ls_columns[0]
    columns = wrap_char + ls_columns[0] + wrap_char + ' AS ' + ls_columns[0]
    for col in ls_columns[1:]:
        # columns = columns + ',`' + col + '` AS ' + col + '\n'
        columns = columns + ',' + wrap_char + col + wrap_char + ' AS ' + col + '\n'

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
