import pyarrow as pa
from airflow.hooks.base_hook import BaseHook
import os
import subprocess


class PyarrowHdfsHook(BaseHook):
    def __init__(self, hdfs_conn_id):
        self.hdfs_conn_id = hdfs_conn_id
        self.conn = None

    def get_conn(self):
        """
        Kết nối đến HDFS và trả về một HadoopFileSystem object
        """
        if self.conn is not None:
            return self.conn

        conn = self.get_connection(self.hdfs_conn_id)
        host = conn.host
        user = conn.login
        port = conn.port
        extra_conf = conn.extra_dejson
        os.environ['JAVA_HOME'] = '/opt/jdk1.8.0_131'
        os.environ['HADOOP_HOME'] = '/opt/hadoop'
        result = subprocess.Popen(['/opt/hadoop/bin/hdfs', 'classpath', '--glob'],
                                  stdout=subprocess.PIPE).communicate()[0]
        os.environ['CLASSPATH'] = str(result)
        self.conn = pa.hdfs.connect(host=host, port=port, user=user, extra_conf=extra_conf)
        return self.conn
