import json
import unittest

from sqlalchemy.orm import Session

from app.lib.employee_etl_process import EmployeeETL
from app.utils.common_util import json_to_obj, str_to_date
from app.utils.postgre_connector import connect_postgres
from schema.output_schema import EmployeeInfo, SalaryInfo
from utils.logger import Logger


class EmployeeETLTest(unittest.TestCase):
    def setUp(self):
        self.greeting = "Start unit test"
        self.session = connect_postgres()
        self.employee_etl = EmployeeETL(self.session)

        with open("C:\\Users\\Sean\\pycharm-project\\code-test-sean-wu\\source\\employee_test.json", "r") as file:
            source_str = file.read()
            self.source_data = json_to_obj(source_str)
        with open("C:\\Users\\Sean\\pycharm-project\\code-test-sean-wu\\source\\employee_error_test.json", "r") as file:
            source_err_str = file.read()
            self.source_data_error = json_to_obj(source_err_str)

    # test connector
    def test_connect_postgres(self):
        try:
            postgres = connect_postgres()
            self.assertTrue(True)
        except Exception as e:
            Logger.info(f"Failed to connect to database: {e}")
            self.assertTrue(False)

    # test API verify source
    def test_verify_source(self):
        # input some correct data and get true
        source_data_list = self.source_data
        for source_data in source_data_list:
            self.assertTrue(self.employee_etl.verify(source_data))

    def test_verify_source_error(self):
        # input some error data and get False
        source_data_list = self.source_data_error
        for source_data in source_data_list:
            self.assertFalse(self.employee_etl.verify(source_data))

    # test API transform data
    def test_employee_generate(self):
        source_data_list = self.source_data

        expected_output_list = self.gen_expected_employee()
        employee_etl = EmployeeETL(self.session)
        for idx in range(len(source_data_list)):
            source_data = source_data_list[idx]
            employee = employee_etl.employee_generate(source_data)
            expected_output = expected_output_list[idx]

            self.assertEqual(employee, expected_output)

    def gen_expected_employee(self):
        expected_output_list = []
        expected_output = EmployeeInfo

        expected_output.employee_id = "abd567"
        expected_output.employee_name = "Sean Wu"
        expected_output.position = "Data Engineer"
        date_str = "2023-04-24T15:09:57.655Z"
        expected_output.joined_date = str_to_date(date_str)
        expected_output.satisfaction_score = 13
        expected_output.is_delete = False

        expected_output_list.append(expected_output)

        return expected_output_list

    def test_salary_generate(self):
        source_data_list = self.source_data
        for source_data in source_data_list:
            salary_list = self.employee_etl.salary_generate(source_data)
            expected_output_list = self.gen_expected_salary()
            for idx in range(len(salary_list)):
                salary = salary_list[idx]
                expected_output = expected_output_list[idx]
                self.assertEqual(salary.salary_value, expected_output.salary_value)

    def gen_expected_salary(self):
        expected_output_list = []

        for idx in range(2):
            expected_output = SalaryInfo()
            if idx == 0:
                expected_output.employee_id = "abd567"
                expected_output.salary_type = "Base"
                expected_output.salary_currency = "GBP"
                expected_output.salary_value = 55000
            elif idx == 1:
                expected_output.employee_id = "abd567"
                expected_output.salary_type = "Bonus"
                expected_output.salary_currency = "USD"
                expected_output.salary_value = 5000
            expected_output_list.append(expected_output)

        return expected_output_list


if __name__ == '__main__':
    tests = unittest.TestLoader().loadTestsFromTestCase(EmployeeETLTest)
    unittest.TextTestRunner(verbosity=2).run(tests)
