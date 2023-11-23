from airflow.models import BaseOperator
from hook.pyarrow_hdfs_hook import PyarrowHdfsHook
from airflow.utils.decorators import apply_defaults
import pandas as pd
from airflow.plugins_manager import AirflowPlugin
import subprocess
from airflow.hooks.oracle_hook import OracleHook
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_CHUNK_SIZE = 500000


class OracleToHdfsOperator(BaseOperator):
    template_fields = ('query', 'schema_raw', 'oracle_conn_id', 'hdfs_conn_id', 'output_path', 'base_path')

    @apply_defaults
    def __init__(self,
                 oracle_conn_id=None,
                 hdfs_conn_id=None,
                 query=None,
                 output_path=None,
                 base_path=None,
                 schema_raw=None,
                 *args, **kwargs):
        super(OracleToHdfsOperator, self).__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.query = query
        self.output_path = output_path
        self.base_path = base_path if base_path else output_path
        self.schema_raw = schema_raw
        self.chunk_size = DEFAULT_CHUNK_SIZE

    def replace_string_none(self, df: pd.DataFrame, schema_raw: dict) -> pd.DataFrame:
        schema_need_replace: dict = {}
        for key in schema_raw:
            if schema_raw[key] == 'str':
                schema_need_replace[key] = 'None'
        if schema_need_replace:
            result_df = df.replace(schema_need_replace, '')
            return result_df
        return df

    def run_hdfs_cli(self, command, message=""):
        print(message)
        print("RUN:", command)
        res = subprocess.run(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if res.returncode != 0:
            raise Exception("ERROR LOGS:", res.stderr)
        else:
            if res.stdout:
                print("OUTPUT:", res.stdout)
            if res.stderr:
                print("DEBUG:", res.stderr)

    def execute(self, context):
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)

        db_conn = oracle_hook.get_conn()
        hdfs_hook = PyarrowHdfsHook(self.hdfs_conn_id)
        hdfs_conn = hdfs_hook.get_conn()

        self.log.info(f'Hdfs base path: {self.base_path}')
        self.log.info(f'Hdfs raw path: {self.output_path}')
        self.log.info(f'Query raw: \n{self.query}')

        # create dir
        cmd_mkdir_output_path = f"/opt/hadoop/bin/hdfs dfs -mkdir -p {self.output_path}"
        self.run_hdfs_cli(command=cmd_mkdir_output_path)

        # chmod 775 base dir after creating output_path
        cmd_chmod_base_path = f"/opt/hadoop/bin/hdfs dfs -chmod -R 775 {self.base_path}"
        self.run_hdfs_cli(
            command=cmd_chmod_base_path,
            message="chmod 775 base dir after creating output_path"
        )

        for df in pd.read_sql(self.query, db_conn, chunksize=self.chunk_size):
            self.log.debug(df.dtypes)
            self.log.info('Cast with schema:')
            df_with_schema = df.astype(self.schema_raw, errors="ignore")
            df_with_replace = self.replace_string_none(df_with_schema, self.schema_raw)
            self.log.info('Schema after cast:')
            self.log.info(df_with_replace.dtypes)
            table = pa.Table.from_pandas(df_with_replace)
            self.log.info('Write file to HDFS:')
            pq.write_to_dataset(table, root_path=self.output_path, filesystem=hdfs_conn)

        # chmod 775 base dir again for sub dirs and files newly created
        cmd_chmod_base_path = f"/opt/hadoop/bin/hdfs dfs -chmod -R 775 {self.base_path}"
        self.run_hdfs_cli(
            command=cmd_chmod_base_path,
            message="chmod 775 base dir again for sub dirs and files newly created"
        )

        # close source database connection to release resource
        db_conn.close()


class OracleToHdfsPlugin(AirflowPlugin):
    name = "oracle_to_hdfs_plugin"
    operators = [OracleToHdfsOperator]
