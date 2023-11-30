class DaoDim:
    """
        Extract SQL template
    :param BUSINESS_DATE: BUSINESS_DATE value
    :type BUSINESS_DATE: str
    :param table_name: table destination name
    :type table_name: str
    :param columns: list of all columns need ETL
    :type columns: List
    :param by_date: date filter condition
    :type by_date: str
    :param ETL_DATE: etl timestamp
    :type ETL_DATE: str
    ::
    """

    def get_sql_statement_extract_data(
        self,
        BUSINESS_DATE,
        table_name,
        columns,
        schema_name="",
        by_date=None,
        value_date=None,
        ETL_DATE="CURRENT_TIMESTAMP()",
    ):
        column_alias = ["{}.{}".format(table_name, column) for column in columns]
        if by_date is None:
            SQL_SELECT = (
                "SELECT "
                "{3} AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} ".format(
                    BUSINESS_DATE, ",".join(column_alias), table_name, ETL_DATE
                )
            )
        else:
            SQL_SELECT = (
                "SELECT "
                "{4} AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} WHERE {3}".format(
                    BUSINESS_DATE, ",".join(column_alias), table_name, by_date, ETL_DATE
                )
            )
        return SQL_SELECT
