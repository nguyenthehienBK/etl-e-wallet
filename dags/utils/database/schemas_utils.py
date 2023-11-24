from utils.database.general_db_adhoc_utils import GroupChar


def is_db_field(col_name):
    """
        Return true if col is table field_name and not be modified
    """
    if col_name[0] != '`' and col_name.find(' ') == -1:
        return True
    return False


def get_field_names(columns_schema, quote=''):
    """
    Return a list of field_names from pg_table_schema
    :type columns_schema: list of dict
    :param columns_schema:  ex.COLUMNS_SCHEMA in class schema_postgres_datamart.py
    :type quote: str
    :param quote: quote char for wrapping fields
    """
    quote = '' if quote is None else quote
    list_columns = []
    for c in columns_schema:
        # if column_name does not have special char (`) or use alias then use quote
        if is_db_field(c['name']):
            col = f"{quote}{c['name']}{quote}"
        else:
            col = c['name']
        list_columns.append(col)
    return list_columns


def gen_upsert_cond_expr(upsert_fields, quote=''):
    """
    Return a str representing condition for UPSERT_FIELDS
    ex. return: a=%s and b=%s

    :type upsert_fields: list of dict
    :param upsert_fields: ex.UPSERT_FIELDS in class schema_postgres_datamart.py
    :type quote: str
    :param quote: quote char for wrapping fields
    """
    upsert_sql_cond = ""
    for key_field in upsert_fields:
        key_col = f"{quote}{key_field['field']}{quote}"
        u_logic_rel = key_field["logic_relation"]
        u_group_char = key_field["group_char"]

        logic_expr = u_logic_rel
        if u_group_char == GroupChar.OPEN:
            logic_expr = f"{u_logic_rel} {GroupChar.OPEN}"
        close_group = GroupChar.CLOSE if u_group_char == GroupChar.CLOSE else GroupChar.EMPTY

        upsert_sql_cond += f" {logic_expr} {key_col}=%s {close_group}"
    return upsert_sql_cond.strip()


def gen_extract_cond_expr(extract_fields, quote=''):
    """
    Return a str representing condition for EXTRACT_FIELDS
    ex. return: a=%s and b=%s

    :type extract_fields: list of dict
    :param extract_fields: ex.EXTRACT_FIELDS, KF_EXTRACT_FIELDS in class schema_postgres_datamart.py
    :type quote: str
    :param quote: quote char for wrapping fields
    """
    upsert_sql_cond = ""
    for key_field in extract_fields:
        key_col = f"{quote}{key_field['field']}{quote}"
        u_logic_rel = key_field["logic_relation"]
        u_group_char = key_field["group_char"]

        logic_expr = u_logic_rel
        if u_group_char == GroupChar.OPEN:
            logic_expr = f"{u_logic_rel} {GroupChar.OPEN}"
        close_group = GroupChar.CLOSE if u_group_char == GroupChar.CLOSE else GroupChar.EMPTY

        upsert_sql_cond += f" {logic_expr} {key_col}=%s {close_group}"
    return upsert_sql_cond.strip()


def get_key_columns(upsert_fields, columns_schema, quote=''):
    """
    return key_fields (a list containing key columns' name)
         , key_field_ids (a list of key columns' id in pg_table_schema)
    using UPSERT_FIELDS in pg_table_schema

    :type upsert_fields: list of dict
    :param upsert_fields: ex.UPSERT_FIELDS in class schema_postgres_datamart.py
    :type columns_schema: list of dict
    :param columns_schema:  ex.COLUMNS_SCHEMA in class schema_postgres_datamart.py
    :type quote: str
    :param quote: quote char for wrapping fields
    """
    if upsert_fields is None or columns_schema is None:
        return [], []
    key_fields = [f"{quote}{f['field']}{quote}" for f in upsert_fields]
    dict_key_fields = dict.fromkeys(key_fields, 1)
    key_field_ids = [col_id for col_id, col in enumerate(columns_schema)
                     if f"{quote}{col['name']}{quote}" in dict_key_fields]
    return key_fields, key_field_ids


def find_table(check_tables, ck_table_name):
    """
    Return table schema (object in postgres_table_schema)
    or None
    :type check_tables: list of source table schemas
    :param check_tables: list, ex: ALL_TABLES in schema_postgres_datamart
    :type ck_table_name: str
    :param ck_table_name:  table_name to check exists
    """
    for table in check_tables:
        # table.TABLE_NAME = target table name
        if table.tgt_table_name.lower() == ck_table_name.lower():
            return table
    return None
