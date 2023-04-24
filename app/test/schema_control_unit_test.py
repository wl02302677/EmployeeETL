import unittest
from app.utils.postgre_connector import connect_postgres
from lib.schema_control_process import SchemaControl


class SchemaControlTest(unittest.TestCase):
    def setUp(self):
        self.greeting = "Start unit test"
        self.conn = connect_postgres()
        self.schema_control = SchemaControl(self.conn)
        self.table = "EmployeeInfoTest"

    # test connector
    def test_connect_postgres(self):
        try:
            postgres = connect_postgres()
            self.assertTrue(True)
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            self.assertTrue(False)

    # test create migrations table
    def test_create_migrations_table(self):
        try:
            self.schema_control.create_migrations_table()
            self.assertTrue(True)
        except Exception as e:
            print(f"Failed to create migrations_table: {e}")
            self.assertTrue(False)

    # test get current version
    def test_get_current_version(self):
        current_version = self.schema_control.get_current_version(self.table)
        self.assertEqual(current_version, 0)

    # test apply migrations
    def test_apply_migrations(self):
        cur = self.conn.cursor()
        table = self.table
        create_table_sql = f"CREATE TABLE {table} (""employee_id VARCHAR(20),salary_type VARCHAR(50)," \
                           "salary_currency VARCHAR(20)," \
                           "salary_value INTEGER,create_date DATE,last_update_date DATE," \
                           "PRIMARY KEY (employee_id, salary_type))"
        self.schema_control.apply_migrations(op_type="create_table", create_sql=create_table_sql, target_table=table)

        # update test
        field = "last_update_date"
        new_field = "update_date"
        self.schema_control.apply_migrations("U", table, field=field, new_field=new_field)

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    ("employeeinfotest",))
        schema = cur.fetchall()
        # make sure update success
        if ("update_date",) in schema:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

        # create test
        field = "manager_id"
        type = "VARCHAR"
        self.schema_control.apply_migrations("C", table, field=field, field_type=type)

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    ("employeeinfotest",))
        schema = cur.fetchall()
        # make sure update success
        if ("manager_id",) in schema:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

        # delete test
        field = "manager_id"
        type = "VARCHAR"
        self.schema_control.apply_migrations("D", table, field=field, field_type=type)

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    ("employeeinfotest",))
        schema = cur.fetchall()
        # make sure update success
        if ("manager_id",) in schema:
            self.assertTrue(False)
        else:
            self.assertTrue(True)

    # test rollback migrations
    def test_rollback_migrations(self):
        # check the target version is which have current flag in the migrations table
        target_table = self.table
        cur = self.conn.cursor()
        target_version = 2

        current_version = self.schema_control.get_current_version(target_table)
        self.schema_control.rollback_migrations(target_version, current_version, target_table)
        new_current_version = self.schema_control.get_current_version(target_table)
        # compare version after rollback with target version
        self.assertEqual(new_current_version, target_version)

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    ("employeeinfotest",))
        schema = cur.fetchall()
        # make sure rollback success
        if ("last_update_date",) in schema:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

        cur.close()

    # test goahead migrations
    def test_goahead_migrations(self):
        # check the target version is which have current flag in the migrations table
        target_table = self.table
        target_version = 4
        cur = self.conn.cursor()

        current_version = self.schema_control.get_current_version(target_table)
        self.schema_control.goahead_migrations(target_version, current_version, target_table)
        new_current_version = self.schema_control.get_current_version(target_table)

        # compare version after rollback with target version
        self.assertEqual(new_current_version, target_version)

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    ("employeeinfotest",))
        schema = cur.fetchall()
        # make sure rollback success
        if ("update_date",) in schema:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

        cur.close()

if __name__ == '__main__':
    tests = unittest.TestLoader().loadTestsFromTestCase(SchemaControlTest)
    unittest.TextTestRunner(verbosity=2).run(tests)
