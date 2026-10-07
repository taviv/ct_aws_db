"""Register the current Parquet snapshot as Glue tables so it can be queried ad hoc from Athena."""

import logging

logger = logging.getLogger(__name__)

_TYPE_MAP = {
    "VARCHAR": "string",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "DOUBLE": "double",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "VARCHAR[]": "array<string>",
}


def glue_type(duckdb_type: str) -> str:
    try:
        return _TYPE_MAP[duckdb_type]
    except KeyError:
        raise ValueError(f"No Glue mapping for DuckDB type {duckdb_type!r}") from None


def register_tables(glue_client, database: str, tables: dict) -> None:
    """``tables``: name → (s3 directory location, [(column, duckdb_type), ...])."""
    for name, (location, columns) in tables.items():
        table_input = {
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "StorageDescriptor": {
                "Columns": [{"Name": c, "Type": glue_type(t)} for c, t in columns],
                "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        }
        try:
            glue_client.update_table(DatabaseName=database, TableInput=table_input)
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_table(DatabaseName=database, TableInput=table_input)
        logger.info("Glue table %s.%s → %s", database, name, location)
