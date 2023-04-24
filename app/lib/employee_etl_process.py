# implement employee type data ETL process
import datetime

from lib.base.base_etl_process import ETL
from schema.output_schema import EmployeeInfo, SalaryInfo
from utils.logger import Logger


class EmployeeETL(ETL):
    def __init__(self, postgres):
        super(EmployeeETL, self).__init__(postgres)
        self.postgres = postgres

    def verify(self, source_info):
        # id is primary key in Employee table, must have name and position
        if "id" not in source_info or "name" not in source_info or "attributes" not in source_info \
                or "position" not in source_info["attributes"] or "salaryValues" not in source_info:
            return False
        for salary in source_info["salaryValues"]:
            # type is one of the primary key in Salary table
            if "type" not in salary:
                return False

        return True

    def save_err(self, source_info):
        try:
            with self.postgres.cursor() as cursor:
                sql = "INSERT INTO EmployeeInfoErr (employee_id, employee_name, create_date, last_update_date) " \
                      "VALUES (%s, %s, %s, %s, %s)"
                now = datetime.datetime.now()
                cursor.execute(sql, (source_info["id"], source_info["name"], now, now))
            self.postgres.commit()
        except Exception as e:
            Logger.error("no error data can keep")


    def employee_generate(self, source_info):

        employee = EmployeeInfo()
        # verify control these fields, no need check here

        employee.employee_id = source_info["id"]
        employee.employee_name = source_info["name"]
        employee.position = source_info["attributes"]["position"]
        # need check if input have these data
        if "joinedOn" in source_info["attributes"]:
            employee.joined_date = source_info["attributes"]["joinedOn"]
        if "satisfactionScore" in source_info["attributes"]:
            employee.satisfaction_score = source_info["attributes"]["satisfactionScore"]
        if "isDeleted" in source_info:
            employee.is_delete = source_info["isDeleted"]

        return employee

    def salary_generate(self, source_info):
        salary_list = []
        bonus_flag = False
        for salary in source_info["salaryValues"]:
            if salary["type"] == "Bonus":
                bonus_flag = True
            salary_info = SalaryInfo()
            # verify control these fields, no need check here
            salary_info.employee_id = source_info["id"]
            salary_info.salary_type = salary["type"]
            # need check if input have these data
            if "currency" in salary:
                salary_info.salary_currency = salary["currency"]
            if "value" in salary:
                salary_info.salary_value = salary["value"]
            salary_list.append(salary_info)

        if not bonus_flag:
            salary_info = SalaryInfo()
            salary_info.employee_id = source_info["id"]
            salary_info.salary_type = "Bonus"
            salary_info.salary_value = 0

        return salary_list

    def output(self, employee_all_list: list[EmployeeInfo], salary_all_list: list[SalaryInfo]):
        for employee in employee_all_list:
            with self.postgres.cursor() as cursor:
                sql = "INSERT INTO EmployeeInfo (employee_id, employee_name, position, joined_date, " \
                      "satisfaction_score, is_delete, create_date, last_update_date) " \
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (employee_id) " \
                      "DO UPDATE SET employee_name = excluded.employee_name, position = excluded.position," \
                      " joined_date = excluded.joined_date, satisfaction_score = excluded.satisfaction_score, " \
                      "last_update_date = excluded.last_update_date"

                now = datetime.datetime.now()
                cursor.execute(sql, (employee.employee_id, employee.employee_name, employee.position,
                                     employee.joined_date, employee.satisfaction_score, employee.is_delete,
                                     now, now))
            self.postgres.commit()

        for salary in salary_all_list:
            with self.postgres.cursor() as cursor:
                sql = "INSERT INTO SalaryInfo (employee_id, salary_type, salary_currency, salary_value, create_date, " \
                      "last_update_date) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (employee_id, salary_type) " \
                      "DO UPDATE SET salary_currency = excluded.salary_currency, salary_value = excluded.salary_value" \
                      ", last_update_date = excluded.last_update_date"
                cursor.execute(sql, (salary.employee_id, salary.salary_type, salary.salary_currency,
                                     salary.salary_value, now, now))
            self.postgres.commit()
