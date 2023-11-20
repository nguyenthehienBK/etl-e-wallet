class SourceDB:
    MYSQL = "MYSQL"


class LogicRelation:
    AND = "and"
    OR = "or"
    EMPTY = ""


class GroupChar:
    OPEN = "("
    CLOSE = ")"
    EMPTY = ""


class DatabaseMappingType:

    BQ_2_SPARK_SQL_TYPE = {
        "INTEGER": "LONG",
        "NUMERIC": "DECIMAL",
        "FLOAT64": "DOUBLE",
        "FLOAT": "DOUBLE",
        "STRING": "STRING",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP",
        "DATETIME": "TIMESTAMP",
        "DATE": "DATE",
    }


class DatabaseMappingOption:
    BQ_2_POSTGRES_MODE = {"REQUIRED": "NOT NULL", "NULLABLE": ""}


class RenderTemplate:
    @staticmethod
    def get_sql_queries(sql, query_params):
        """
        :param sql: path to file .sql or a list string of query sql
                    ex. "dags/sql/dwh/generic/template.sql"
        :type sql: str or list[str]
        :type query_params: dict
        """
        # sql_script can contain multiple sql queries splited by ';\n'
        if isinstance(sql, str):
            with open(sql) as fi:
                lst_sql_script = [fi.read()]
        else:
            lst_sql_script = sql

        # replace params in sql_scripts with values
        sql_queries = []
        for sql_script in lst_sql_script:
            for pk, pv in query_params.items():
                pk = "{{params.%s}}" % pk
                sql_script = sql_script.replace(pk, pv)
            sql_queries.append(sql_script)
        sql_queries = [sql_query for sql_query in sql_queries if sql_query.strip()]

        return sql_queries
