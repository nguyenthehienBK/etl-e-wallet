from schema.common.dao_dim import DaoDim
from schema.common.model import BaseModel, FACT_TABLE_TYPE, DIM_TABLE_TYPE


class MartTires(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.SCHEMA_ICEBERG = [
            {"name": "id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "description", "mode": "NULLABLE", "type": "string"},
            {"name": "role_type_id", "mode": "NULLABLE", "type": "bigint"},
            {"name": "status", "mode": "NULLABLE", "type": "string"},
            {"name": "tier_code", "mode": "NULLABLE", "type": "string"},
            {"name": "tier_name", "mode": "NULLABLE", "type": "string"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "bigint"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "bigint"},
        ]

        self.SCHEMA_MYSQL = [
            {"name": "id", "mode": "NULLABLE", "type": "int"},
            {"name": "description", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "role_type_id", "mode": "NULLABLE", "type": "int"},
            {"name": "status", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "tier_code", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "tier_name", "mode": "NULLABLE", "type": "varchar(255)"},
            {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "created_by", "mode": "NULLABLE", "type": "int"},
            {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "updated_by", "mode": "NULLABLE", "type": "int"},
            {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
            {"name": "deleted_by", "mode": "NULLABLE", "type": "int"},
        ]

        self.KEY_COLUMNS = [
            {"name": "id", "type": "bigint"}
        ]
        self.TIME_PARTITIONING = None
        self.MIGRATION_TYPE = 'SQL_ID'
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.EXTRACT = {
            "TIMESTAMP": "",
            "TIMESTAMP_KEY": "",
            "ORDER_BY": "id",
            "JOIN": ""
        }


MART_TIERS = "mart_tiers"

W3_CORE_MDM_TABLE_SCHEMA = {
    MART_TIERS: MartTires(MART_TIERS)
}

_ALL_DIM = [
    MART_TIERS
]

_ALL_FACT = [

]

_ALL_TABLE = _ALL_DIM + _ALL_FACT
