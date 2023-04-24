import requests
import json

from psycopg2.extras import DictCursor

from app.lib.employee_etl_process import EmployeeETL
from app.schema.output_schema import EmployeeInfo, SalaryInfo
from app.utils.postgre_connector import connect_postgres

# call api and query test postgres to get result
with open("C:\\Users\\Sean\\pycharm-project\\code-test-sean-wu\\source\\employee_test.json", "r") as file:
    json_data = file.read()

url = 'http://127.0.0.1:8000/ProductDetailetl/employeeETL'
headers = {'Content-Type': 'application/json'}
params = {'source_info_json': json_data}
response = requests.post(url, headers=headers, params=params)
print(response)

if response.status_code == 200:
    print('API request success')
    print('response：', response)
    postgres = connect_postgres()
    cur = postgres.cursor(cursor_factory=DictCursor)

    sql = "SELECT * FROM EmployeeInfo where employee_id = %s"
    params = ('abd567',)
    cur.execute(sql, params)
    employees = cur.fetchall()

    cur.close()
    postgres.close()

    for employee in employees:
        if employee["employee_name"] == "Sean Wu":
            print('pass the test')
        else:
            print('something wrong, need to check!!!')
else:
    print('API request failed')
    print('error status：', response.status_code)


