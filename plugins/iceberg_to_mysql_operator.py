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
            *args,
            **kwargs,
    ):
        super(IcebergToMysqlOperator, self).__init__(*args, **kwargs)
        self.sql = sql[len("dags/"):] if sql.find("dags/") != -1 else sql
        self.hive_server2_conn_id = hive_server2_conn_id
        self.mysql_conn_id = mysql_conn_id

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
        self.log.info("Execute SQL: ")
        self.log.info(sql)

        conn = get_spark_thrift_conn(self.hive_server2_conn_id)
        cursor = conn.cursor()
        cursor.execute(sql)
        res = []
        for row in cursor:
            print(row)
            res.append(row)

        cursor.close()
        conn.close()

        return res

    def execute(self, context):
        df_data = self._query()
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql_hook.get_conn()
        conn.set_autocommit(conn, False)
        self.log.info(df_data)


class IcebergToMysqlPlugin(AirflowPlugin):
    name = "iceberg_to_mysql_plugin"
    operators = [IcebergToMysqlOperator]
