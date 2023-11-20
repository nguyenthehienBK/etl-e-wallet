from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from utils.spark_thrift.connections import get_spark_thrift_conn
from utils.database.spark_sql_adhoc_utils import SparkSqlAdhoc
from utils.database import schemas_utils
from utils.iceberg import iceberg_properties_utils
from utils.iceberg.iceberg_config_utils import (
    max_partition_num_line,
    num_retention_snapshot,
)
import math


class SourceFileToIcebergOperator(BaseOperator):
    """
    Fetches data from source file and write to iceberg table (raw data for lakehouse | L0 zone)

    :param source_file_uri: data source location
    :type source_file_uri: str
    :param iceberg_table_uri: target table location
    :type iceberg_table_uri: str
    :param iceberg_table_schema: schema of iceberg table
    :type iceberg_table_schema: object (instance of schema class)
    :param hive_server2_conn_id: airflow connection for spark thrift
    :type hive_server2_conn_id: str
    :param source_file_format: source file/source data format (ex. parquet, json)
    :type source_file_format: str
    :param source_file_options: options of data source which will be injected to storage properties.
    :type source_file_options: dict
    :param iceberg_db: database name in iceberg
                       ex. "iceberg.tdb"
    :type iceberg_db: str
    :param str_timetz_expire_snaps: string of timestamp with timezone using "SparkSqlAdhoc.get_str_of_timetz"
                                    if it is None then using str_timetz_expire_snaps = [current_date + 1 days]
                                    ex: 2022-12-24 00:00:00.000000+0000
    :type str_timetz_expire_snaps: str
    :param num_keep_retention_snaps: number of snapshots to keep from older than
                                     default: iceberg_config_utils.num_keep_retention_snaps
    :type num_keep_retention_snaps: int
    :param iceberg_table_props: iceberg table properties
    ref: 'iceberg.apache.org/docs/latest/configuration'
    :type iceberg_table_props: dict (key is property name, value is property value)
    :param iceberg_write_truncate: True for insert overwrite, False for merge (require key columns)
                                   Default: False
    :type iceberg_write_truncate: boolean
    """

    template_fields = (
        "source_file_uri",
        "source_file_format",
        "source_file_options",
        "iceberg_db",
        "iceberg_table_uri",
    )
    ui_color = "#e4f0e8"

    @apply_defaults
    def __init__(
        self,
        source_file_uri,
        iceberg_table_uri,
        iceberg_table_schema,
        hive_server2_conn_id,
        source_file_format="parquet",
        source_file_options=None,
        iceberg_db="default",
        str_timetz_expire_snaps=None,
        num_keep_retention_snaps=num_retention_snapshot,
        iceberg_table_props=None,
        iceberg_write_truncate=False,
        hive_num_partition=None,
        *args,
        **kwargs,
    ):
        super(SourceFileToIcebergOperator, self).__init__(*args, **kwargs)
        self.source_file_uri = source_file_uri
        # clean uri by removing / at the end of target_table_uri if exists
        self.iceberg_table_uri = (
            iceberg_table_uri
            if iceberg_table_uri[-1] != "/"
            else iceberg_table_uri[:-1]
        )
        self.iceberg_table_schema = iceberg_table_schema
        self.hive_server2_conn_id = hive_server2_conn_id
        self.source_file_format = source_file_format
        self.source_file_options = source_file_options
        self.iceberg_db = iceberg_db
        self.str_timetz_expire_snaps = (
            str_timetz_expire_snaps
            if str_timetz_expire_snaps is not None
            else SparkSqlAdhoc.get_str_of_timetz(days=1)
        )
        self.num_keep_retention_snapshot = num_keep_retention_snaps
        self.iceberg_table_props = iceberg_table_props
        self.iceberg_write_truncate = iceberg_write_truncate
        self.hive_num_partition = hive_num_partition

    def get_iceberg_conn(self):
        conn = get_spark_thrift_conn(self.hive_server2_conn_id)
        return conn

    def call_setup_iceberg_table_props(self):
        # if not exists then init
        if self.iceberg_table_props is None:
            self.iceberg_table_props = dict()

        # setup iceberg table properties
        general_props = iceberg_properties_utils.get_general_iceberg_table_props()
        for k, v in general_props.items():
            if k not in self.iceberg_table_props:
                self.iceberg_table_props[k] = v

    def create_external_tmp_dbdata_tbl(self, cursor, tmp_dbdata_tbl_name):
        """
        Create hive external tmp database data table

        :param cursor: hive2_cursor
        :type cursor: obj
        :param tmp_dbdata_tbl_name: table_name
        :type tmp_dbdata_tbl_name: str
        """
        if self.source_file_format.find("parquet") != -1:
            # getting schema from data location
            # this is due to parquet strict schema (field types)
            # ex. CREATE TABLE if not exists default.table_name
            # USING data_format
            # LOCATION source_file_uri
            external_tmp_dbdata_tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
                table_name=tmp_dbdata_tbl_name,
                table_columns_schema=self.iceberg_table_schema.COLUMNS_SCHEMA,
                location=self.source_file_uri,
                data_format="parquet",
                table_field_expr="",
            )
        else:
            # getting schema from COLUMNS_SCHEMA
            # ex. CREATE TABLE if not exists default.table_name (id bigint)
            # USING data_format
            # LOCATION source_file_uri;
            external_tmp_dbdata_tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
                table_name=tmp_dbdata_tbl_name,
                table_columns_schema=self.iceberg_table_schema.COLUMNS_SCHEMA,
                location=self.source_file_uri,
                data_format=self.source_file_format,
                data_options=self.source_file_options,
            )
        # print(external_tmp_dbdata_tbl_sqls[0])
        cursor.execute(f"{external_tmp_dbdata_tbl_sqls[0]}")

    def call_expire_snapshots(self, cursor):
        """
        expire snapshots from target iceberg table

        :param cursor: database connection cursor
        type cursor: obj
        """
        snapshot_del_sql = f"""
        CALL iceberg.system.expire_snapshots (
            table => '{self.iceberg_db}.{self.iceberg_table_schema.TABLE_NAME}', 
            older_than => TIMESTAMP '{self.str_timetz_expire_snaps}', 
            retain_last => {self.num_keep_retention_snapshot}
        )
        """
        print(
            f"\nKeep {self.num_keep_retention_snapshot} latest snapshots\n",
            snapshot_del_sql,
        )
        cursor.execute(snapshot_del_sql)

    def call_remove_orphan_files(self, cursor):
        """
        remove orphan files (metadata and data files not used) from target iceberg table

        :param cursor: database connection cursor
        type cursor: obj
        """
        timetz_str_remove_files = SparkSqlAdhoc.get_str_of_timetz(days=-2)
        orphan_files_del_sql = f"""
        CALL iceberg.system.remove_orphan_files (
            table => '{self.iceberg_db}.{self.iceberg_table_schema.TABLE_NAME}', 
            older_than => TIMESTAMP '{timetz_str_remove_files}'
        )
        """
        print(
            f"\nRemove orphan files older than {timetz_str_remove_files} \n",
            orphan_files_del_sql,
        )
        cursor.execute(orphan_files_del_sql)

    def execute(self, context):
        # init iceberg connection
        conn = self.get_iceberg_conn()
        cursor = conn.cursor()

        # init iceberg table properties
        self.call_setup_iceberg_table_props()

        # create database if not exists
        db_create_sql = f"create database if not exists {self.iceberg_db};"
        # print(db_create_sql)
        cursor.execute(db_create_sql)

        # create output iceberg table
        tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
            table_name=self.iceberg_table_schema.TABLE_NAME,
            table_columns_schema=self.iceberg_table_schema.COLUMNS_SCHEMA,
            db_name=self.iceberg_db,
            location=self.iceberg_table_uri,
            partitions=self.iceberg_table_schema.TIME_PARTITIONING,
            data_format="iceberg",
            table_properties=self.iceberg_table_props,
            normalize_to_str=False,
        )
        # print(tbl_sqls[0])
        cursor.execute(f"{tbl_sqls[0]}")

        # tmp_dbdata_tbl_name is external table name
        # this stores source data at specific date
        ori_dbname = self.iceberg_db.split(".")[-1]
        tmp_dbdata_tbl_name = (
            f"{ori_dbname}_{self.iceberg_table_schema.TABLE_NAME}_dbdata_tmp"
        )

        # prepare
        SparkSqlAdhoc.drop_table_if_exists(
            cursor=cursor, db_name="default", table_name=tmp_dbdata_tbl_name
        )

        # get iceberg table columns
        cols_for_select = self.iceberg_table_schema.get_list_columns(
            self.iceberg_table_schema.COLUMNS_SCHEMA
        )
        default_columns = []
        cols_for_select = default_columns + cols_for_select
        cols_expr_for_select = ",".join(cols_for_select)
        src_cols = [f"s.{col}" for col in cols_for_select]
        src_col_expr = ",".join(src_cols)
        src_tgt_update_cols = [f"t.{k}=s.{k}" for k in cols_for_select]
        src_tgt_update_col_expr = ",".join(src_tgt_update_cols)

        # get iceberg table key columns
        key_fields = schemas_utils.get_field_names(
            self.iceberg_table_schema.KEY_COLUMNS, quote="`"
        )
        src_tgt_keys = [f"t.{k}=s.{k}" for k in key_fields]
        src_tgt_key_expr = " and ".join(src_tgt_keys)

        # create_external_tmp_dbdata_tbl
        self.create_external_tmp_dbdata_tbl(cursor, tmp_dbdata_tbl_name)

        # write data to target iceberg table

        # detect num partition
        cursor.execute(
            f"""
            select count(1) from default.{tmp_dbdata_tbl_name};
            """
        )
        hive_num_line = float(cursor.fetchall()[0][0])
        hive_num_partition = (
            self.hive_num_partition
            if self.hive_num_partition
            else math.ceil(hive_num_line / max_partition_num_line)
        )

        if self.iceberg_write_truncate:
            # insert overwrite data to target table
            insert_overwrite_sql = f"""INSERT OVERWRITE {self.iceberg_db}.{self.iceberg_table_schema.TABLE_NAME} ({cols_expr_for_select})
            SELECT /*+ REPARTITION({hive_num_partition}) */ distinct {cols_expr_for_select} 
            FROM default.{tmp_dbdata_tbl_name};
            """
            # print(insert_overwrite_sql)
            cursor.execute(insert_overwrite_sql)
        else:
            # merge to get latest record
            merge_sql = f"""MERGE INTO {self.iceberg_db}.{self.iceberg_table_schema.TABLE_NAME} t
            USING (SELECT /*+ REPARTITION({hive_num_partition}) */ {cols_expr_for_select} FROM default.{tmp_dbdata_tbl_name} ) s         
            ON {src_tgt_key_expr}          
            WHEN MATCHED THEN UPDATE SET {src_tgt_update_col_expr}
            WHEN NOT MATCHED THEN INSERT ({cols_expr_for_select}) VALUES ({src_col_expr});
            """
            # print(merge_sql)
            cursor.execute(merge_sql)

            # clean unused files (data, metadata)
            self.call_expire_snapshots(cursor)
            self.call_remove_orphan_files(cursor)

        # drop all tmp tables
        SparkSqlAdhoc.drop_table_if_exists(
            cursor=cursor, db_name="default", table_name=tmp_dbdata_tbl_name
        )

        # release iceberg connection
        cursor.close()
        conn.close()


class SourceFileToIcebergOperatorPlugin(AirflowPlugin):
    name = "source_file_to_iceberg_plugin"
    operators = [SourceFileToIcebergOperator]
