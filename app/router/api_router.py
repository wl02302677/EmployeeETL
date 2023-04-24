from fastapi import APIRouter, Depends, HTTPException, status
from router.apis.employee_etl import router as employee_etl_router
from router.apis.schema_control import router as schema_control_router

api_router = APIRouter()
api_router.include_router(employee_etl_router, prefix="/ETL", tags=["ETL"])
api_router.include_router(schema_control_router, prefix="/Schema", tags=["SchemaControl"])
