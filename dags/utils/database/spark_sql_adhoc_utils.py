from utils.database.general_db_adhoc_utils import (
    DatabaseMappingType,
)
from utils.database import schemas_utils
from utils.database.db_data_type import UpsertType
from utils.date_time.date_time_utils import (
    get_business_date,
    get_timestamp_tz_from_datestr,
    float_to_datetime,
    date_2_str,
)


def gen_partition(table_columns_dict, normalize_to_str, partition_row):
    col_name = partition_row["field"]

    # adding col_type if col_name not in table_columns_dict
    # meaning create that col in partition_expr
    # else col_type is empty
    col_type = ""
    if col_name not in table_columns_dict:
        if normalize_to_str:
            col_type = "STRING"
        else:
            # col_type = mapper_2_spark_sql_type[partition_row["type"]]
            col_type = partition_row["type"]

    # add field to partition_fields
    if partition_row["type"] == UpsertType.DAY:
        field_expr = f"days({col_name}) {col_type}"
    else:
        field_expr = f"{col_name} {col_type}"

    return field_expr


class SparkSqlAdhoc:
    @staticmethod
    def gen_create_tbl_sqls(
        table_name,
        table_columns_schema,
        db_name="default",
        location=None,
        partitions=None,
        data_format="json",
        data_options=None,
        table_field_expr=None,
        table_properties=None,
        normalize_to_str=False,
    ):
        """
        Generate: create table query if not exists, create partitioning queries for table
        :param table_name
        :type table_name: str
        :param table_columns_schema: column schema of table in Bigquery format
        :type table_columns_schema: list of dict(name, mode, type) of fields
        :param db_name: use for db_name.table_name when create table
        :type db_name: str
        :param location: table location
                         set to None for managed table
        :type location: str
        :param partitions: fields for partitioning
        :type partitions: can be a list of dict or a dict
        :param data_format: table stored in json, parquet, iceberg
                       + org.apache.spark.sql.json
                       + org.apache.spark.sql.parquet
        :type data_format: str
        :param data_options: options of data source which will be injected to storage properties.
        :type data_options: dict
        :param table_field_expr: set empty str for getting schema from data stored in location
                                 else from table_columns_schema
        :type table_field_expr: str
        :param table_properties
        :type table_properties: dict
        :param normalize_to_str: True then all col types will be cast to String
        :type normalize_to_str: boolean
        """

        # the sql which this func is building
        # Note:
        #  [OPTIONS, LOCATION, PARTITION, TBLPROPERTIES] will be appended then after
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} {field_expr}
         USING {spark_format}
        """

        mapper_2_spark_sql_type = DatabaseMappingType.BQ_2_SPARK_SQL_TYPE
        # sqls is the list sql returned
        sqls = []

        # check condition to continue processing
        if partitions and table_field_expr is not None:
            raise Exception(
                """
            only one of partitions or table_field_expr can be specified value
            partition is used only when field_expr exists
            """
            )

        # spark_format is table format
        if data_format == "json":
            spark_format = "org.apache.spark.sql.json"
        elif data_format == "parquet":
            # spark_format = "org.apache.spark.sql.parquet"
            spark_format = "PARQUET"
        else:
            spark_format = data_format

        # infer data_options
        if data_options:
            parsed_data_options = []
            for k, v in data_options.items():
                parsed_data_options += [f'"{k}"="{v}"']
            data_options_expr = ", ".join(parsed_data_options)
            create_table_sql += f" OPTIONS ({data_options_expr}) \n"

        # set partition for table
        table_columns = schemas_utils.get_field_names(table_columns_schema)
        table_columns_dict = dict.fromkeys(table_columns, 1)
        if isinstance(partitions, list):
            partition_fields = []
            for row in partitions:
                field_expr = gen_partition(table_columns_dict, normalize_to_str, row)
                partition_fields.append(field_expr)
            partition_expr = ",".join(partition_fields)
            # append partition to create_table_sql
            create_table_sql += f" PARTITIONED BY ({partition_expr}) \n"
        elif isinstance(partitions, dict):
            partition_expr = gen_partition(
                table_columns_dict, normalize_to_str, partitions
            )
            create_table_sql += f" PARTITIONED BY ({partition_expr}) \n"

        # generate schema for table (field_expr)
        # if table_field_expr is not specified value then generate all_field_expr from table_columns_schema
        if table_field_expr is None:
            lst_field = []
            for colr in table_columns_schema:
                col_name = (
                    f'`{colr["name"]}`'
                    if schemas_utils.is_db_field(colr["name"])
                    else colr["name"]
                )
                # normalize_to_str means that all col type is string
                if normalize_to_str:
                    col_type = "STRING"
                else:
                    # col_type = mapper_2_spark_sql_type[colr["type"]]
                    col_type = colr["type"]
                field_expr = f"{col_name} {col_type}"
                lst_field.append(field_expr)
            # create fields_expr separated by \n
            all_field_expr = ",\n ".join(lst_field)
            # wrap by ()
            all_field_expr = f"({all_field_expr})"
        else:
            # set all_field_expr as empty, so the table schema will be got from data
            all_field_expr = table_field_expr

        # build tbl_properties
        if table_properties:
            tbl_props = []
            for k, v in table_properties.items():
                prop = f"'{k}'='{v}'"
                tbl_props.append(prop)
            # comma separated props
            tbl_props_expr = ", ".join(tbl_props)
            # wrap by ()
            tbl_props_expr = f"({tbl_props_expr})"
            create_table_sql += f" TBLPROPERTIES {tbl_props_expr} \n"

        # complete create_table_sql
        # print(db_name, table_name, all_field_expr, spark_format)
        # print(create_table_sql)
        create_table_sql = create_table_sql.format(
            db_name=db_name,
            table_name=table_name,
            field_expr=all_field_expr,
            spark_format=spark_format,
        )

        # if location is specified, creating external table else  creating managed table
        # put this after '.format' avoid keyword {} error
        if location:
            create_table_sql += f" LOCATION '{location}' \n"

        create_table_sql = create_table_sql.strip()
        sqls.append(create_table_sql)
        return sqls

    @staticmethod
    def drop_table_if_exists(cursor, db_name, table_name, is_purge=False):
        """
        :param cursor: database connection cursor
        :type cursor: obj
        :type db_name: str
        :type table_name: str
        :param is_purge: is drop with purge or not
        :type is_purge: boolean
        """
        purge_opt = "purge" if is_purge else ""
        drop_table_sql = f"""
                drop table if exists {db_name}.{table_name} {purge_opt};
                """
        print(drop_table_sql)
        cursor.execute(drop_table_sql)

    @staticmethod
    def get_str_of_timetz(days, tz=None):
        """
        return a string representing timestamp with tz

        param days: number of days add or remove from now()
        type days: int
        param tz: number of days add or remove from now()
        type tz: datetime.timezone
        """
        yyyymmdd = get_business_date(days=days)
        if tz:
            # use tz from func param
            timetz_int = get_timestamp_tz_from_datestr(yyyymmdd, hour=0, tz=tz)
        else:
            # use UTC timezone
            timetz_int = get_timestamp_tz_from_datestr(yyyymmdd, hour=0)
        timetz_dt = float_to_datetime(timetz_int)
        timetz_str = date_2_str(timetz_dt, "%Y-%m-%d %H:%M:%S.%f%z")
        return timetz_str
