"""
Build an API that can use the feature of schema_control_process directly
Normally don't use the API to modify DB schema
"""
from fastapi import APIRouter

from lib.schema_control_process import SchemaControl
from utils.logger import Logger
from utils.postgre_connector import connect_postgres

router = APIRouter()


# Normally don't use the API to modify DB schema
@router.post("/apply_migrations", status_code=200)
async def apply_migrations(op_type, target_table, field=None, field_type=None, new_field=None, create_sql=None):
    postgre = connect_postgres()
    schema_control = SchemaControl(postgre)
    schema_control.apply_migrations(op_type, target_table, field=field, field_type=field_type, new_field=new_field,
                                    create_sql=create_sql)
    result_message = f"operation type: {op_type} migration apply finish"
    Logger.info(result_message)

    return result_message


# Normally don't use the API to modify DB schema
@router.post("/migrations", status_code=200)
async def migrations(target_version: int, target_table: str):
    postgre = connect_postgres()
    schema_control = SchemaControl(postgre)
    schema_control.migration(target_version, target_table)
    result_message = f"target_table: {target_table}, target version: {target_version} version migration finish"
    Logger.info(result_message)

    return result_message
