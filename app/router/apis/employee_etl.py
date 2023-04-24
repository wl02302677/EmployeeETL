from schema.output_schema import EmployeeInfo
from utils.logger import Logger
from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile, BackgroundTasks
from fastapi import FastAPI
import time
from pydantic import BaseModel
from lib.employee_etl_process import EmployeeETL
from utils.common_util import json_to_obj
from utils.postgre_connector import connect_postgres

# employee ETL API using fastAPI

router = APIRouter()


@router.post("etl/employeeETL", tags=["ETL"], status_code=200)
async def employeeEtl(source_info_json: str):
    # Logger.info("single process")
    start = time.time()
    postgres = connect_postgres()
    employee_all_list = []
    salary_all_list = []
    batch_cnt = 0
    cnt = 0

    Logger.info(f"Method:: Employee ETL")
    employee_etl = EmployeeETL(postgres)

    if not source_info_json:
        Logger.warn(f"There are no input data. Please check the input!")
        raise HTTPException(status_code=400, detail=f"There are no input data. Please check the input!")

    # json process
    source_info_list = json_to_obj(source_info_json)
    for source_info in source_info_list:
        # verify data format
        if not employee_etl.verify(source_info):
            Logger.warn(f"There are some input missing important data field. Please check the input!")
            # save error data into error record db
            employee_etl.save_err(source_info)
            return

        # generate employee, salary object
        employee = employee_etl.employee_generate(source_info)
        employee_all_list.append(employee)
        salary_list = employee_etl.salary_generate(source_info)
        salary_all_list += salary_list

        batch_cnt += 1
        cnt += 1
        # 1000/batch, output and clear list if exceed
        if batch_cnt >= 1000:
            employee_etl.output(employee_all_list, salary_all_list)
            employee_all_list.clear()
            salary_all_list.clear()
            batch_cnt = 0

    # output employee, salary to postgres
    employee_etl.output(employee_all_list, salary_all_list)

    postgres.close()

    end = time.time()
    result_message = f"Process data count: {cnt}, total time: {end - start}"
    Logger.info(result_message)
    return result_message

