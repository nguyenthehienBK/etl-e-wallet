from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.hooks.mysql_hook import MySqlHook
from utils.spark_thrift.connections import get_spark_thrift_conn
import pandas as pd


class IcebergToMysqlOperator(BaseOperator):
    """
    Query data from Iceberg then load into Mysql table:
        . returned pandas DataFrame query result

    :param sql: sql query or file path to sql query to get data from Iceberg tables
                ex. sql/template/execute_query_iceberg.sql
    :type sql: str
    :param hive_server2_conn_id
    :type hive_server2_conn_id: str
    :param mysql_conn_id
    :type mysql_conn_id: str
    """

    template_fields = ["sql", "hive_server2_conn_id", "mysql_conn_id"]
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
            self,
            sql,
            hive_server2_conn_id,
            mysql_conn_id,
            mysql_database,
            mysql_table_name,
            mysql_schema,
            *args,
            **kwargs,
    ):
        super(IcebergToMysqlOperator, self).__init__(*args, **kwargs)
        self.sql = sql[len("dags/"):] if sql.find("dags/") != -1 else sql
        self.hive_server2_conn_id = hive_server2_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.mysql_table_name = mysql_table_name
        self.mysql_schema = mysql_schema
        self.mysql_database = mysql_database

    def _query(self):
        """
        Queries Iceberg table and returns a pandas DataFrame of results

        Returns:
            pandas.DataFrame:
                :class:`~pandas.DataFrame` populated with row data and column
                headers from the query results. The column headers are derived
                from the destination table's schema.
        """
        sql = self.sql
        conn = get_spark_thrift_conn(self.hive_server2_conn_id)
        cursor = conn.cursor()
        cursor.execute(sql)
        res = []
        for row in cursor:
            res.append(row)
        cursor.close()
        conn.close()
        return res

    def execute(self, context):

        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql_hook.get_conn()

        self.log.info(f"Create table {self.mysql_table_name} MySQL")
        self.log.info(self.generate_sql_create_tbl())
        cursor_create = conn.cursor()
        cursor_create.execute(self.generate_sql_create_tbl())
        cursor_create.close()

        mysql_hook_2 = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn_2 = mysql_hook_2.get_conn()
        self.log.info("Insert to table MySQL")
        insert_sql = self.generate_sql_insert()
        self.log.info(insert_sql)
        cursor_insert = conn_2.cursor()
        cursor_insert.execute(insert_sql)
        cursor_insert.close()

    def generate_sql_create_tbl(self):
        list_col_schema = []
        for col in self.mysql_schema:
            nullable = "NOT NULL" if col.get("mode").upper() == "REQUIRED" else "NULL"
            col_schema = f""" `{col.get("name")}` {col.get("type")} {nullable} """
            list_col_schema.append(col_schema)
        schema = (str(list_col_schema)
                  .replace("[", "(")
                  .replace("]", ")")
                  .replace("'", "")
                  )
        sql_create_tbl = f"CREATE TABLE IF NOT EXISTS `{self.mysql_database}`.`{self.mysql_table_name}` {schema}"
        return sql_create_tbl

    def generate_sql_insert(self):
        df_data = self._query()
        values = str(df_data).replace("[", "").replace("]", "")
        insert_sql = f"INSERT INTO `{self.mysql_database}`.`{self.mysql_table_name}` {self.get_list_column_mysql()} VALUES {values}"
        return insert_sql

    def get_list_column_mysql(self):
        cols = []
        for col in self.mysql_schema:
            cols.append(col.get("name"))
        str_cols = str(cols).replace("[", "(").replace("]", ")").replace("'", "")
        return str_cols


class IcebergToMysqlPlugin(AirflowPlugin):
    name = "iceberg_to_mysql_plugin"
    operators = [IcebergToMysqlOperator]
