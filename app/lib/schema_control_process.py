import logging

from psycopg2.extras import RealDictCursor

from utils.logger import Logger
from utils.postgre_connector import connect_postgres

"""
When modify schema
1. record each different type of operate
2. generate the sql by parameter
When Roll back to specific version EX: 5(do drop) rollback to 3(do create) include 4(do update)
Roll back operate: 5(do create) -> 4(do drop) -> 3(do update back)
1. Get the op_type, table, operate to generate sql
2. if the version of operate want to roll back is create table, error
"""

# logging.getLogger('matplotlib').setLevel(logging.DEBUG)
# logger = logging.getLogger(__name__)


class SchemaControl:
    def __init__(self, postgres):
        self.postgres = postgres

    # Build the schema migration table
    def create_migrations_table(self):

        cur = self.postgres.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id SERIAL PRIMARY KEY,
                version INTEGER NOT NULL,
                op_type VARCHAR,
                sql VARCHAR,
                rollback_sql VARCHAR,
                current Bool,
                target_table VARCHAR,
                create_date TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        self.postgres.commit()
        Logger.info("Create migration success")

    # Get the current version
    def get_current_version(self, target_table):
        cur = self.postgres.cursor()
        cur.execute("SELECT version FROM migrations WHERE target_table = %s and current = %s", (target_table, True))
        result = cur.fetchone()
        if result is not None:
            if result[0] is not None:
                return result[0]
        else:
            return 0

    # Apply new schema migration
    def apply_migrations(self, op_type, target_table, field=None, field_type=None, new_field=None, create_sql=None):
        # op_type, table, operate
        current_version = self.get_current_version(target_table)
        version = current_version + 1

        # 0424 If rollback and apply new migration, remove original branch, use this branch to be the main branch
        cur = self.postgres.cursor()
        cur.execute(
            "SELECT version FROM migrations WHERE target_table = %s AND version = %s "
            , (target_table, version))
        old_branch = cur.fetchone()
        if old_branch:
            cur.execute(
                "DELETE FROM migrations WHERE version >= %s"
                , (version,))

        if op_type == "create_table":
            migration_sql = create_sql
            rollback_sql = None
        else:
            migration_sql, rollback_sql = self.gen_migration_sql(op_type, target_table, field, field_type, new_field)

        # Do this migration
        cur.execute(migration_sql)

        # TODO: need to record parent version
        # Remove current point at this moment
        cur.execute("UPDATE migrations SET current = False WHERE version = %s", (current_version,))
        # Save this migration information to migrations table
        cur.execute("INSERT INTO migrations (version, op_type, sql, rollback_sql, current, target_table) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (version, op_type, migration_sql, rollback_sql, True, target_table))
        self.postgres.commit()
        # Logger.info(f"Applied migration {migration_sql}")
        print(f"Applied migration {migration_sql}")

    # modify version
    def migration(self, target_version, target_table):
        """
            goahead
            . Create
            . Update
            -> current
            . Delete
            . Create (R: Delete)
            . Update
            rollback
        """
        current_version = self.get_current_version(target_table)
        if target_version > current_version:
            print("Run go ahead migration")
            self.goahead_migrations(target_version, current_version, target_table)
        elif target_version < current_version:
            print("Run rollback migration")
            self.rollback_migrations(target_version, current_version, target_table)

    # rollback to specific version of schema
    def rollback_migrations(self, target_version, current_version, target_table):
        """
            1. find the difference step from now to rollback version
            2. do rollback operation for every history step exclude target rollback operation
        """
        cur = self.postgres.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT rollback_sql, version FROM migrations WHERE target_table = %s AND version BETWEEN %s and %s "
            "order by version desc", (target_table, target_version + 1, current_version))
        migration_list = cur.fetchall()
        # do rollback operation for every history step
        temp_version = current_version
        try:
            for migration in migration_list:
                print(migration["version"])
                print(migration["rollback_sql"])
                # Record temp version each time
                temp_version = migration["version"]
                cur.execute(migration["rollback_sql"])
                self.postgres.commit()
        except Exception:
            Logger.error(f"Failed update all step, keep in version: {temp_version}")
            cur.execute("UPDATE migrations SET current = False WHERE version = %s", (current_version,))
            cur.execute("UPDATE migrations SET current = True WHERE version = %s", (temp_version,))

        # Modify current flag in migrations table
        cur.execute("UPDATE migrations SET current = False WHERE version = %s", (current_version,))
        cur.execute("UPDATE migrations SET current = True WHERE version = %s", (target_version,))
        self.postgres.commit()

    # go ahead to specific version of schema
    def goahead_migrations(self, target_version, current_version, target_table):
        """
            1. find the difference step from now to goahead version
            2. do operation for every step exclude current operation
        """
        cur = self.postgres.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT sql, version FROM migrations WHERE target_table = %s AND version BETWEEN %s and %s"
            " order by version asc", (target_table, current_version + 1, target_version))
        migration_list = cur.fetchall()
        # do operation for every step
        temp_version = current_version
        try:
            for migration in migration_list:
                print(migration["sql"])
                # Record temp version each time
                temp_version = migration["version"]
                cur.execute(migration["sql"])
                self.postgres.commit()
        except Exception:
            Logger.error(f"Failed update all step, keep in version: {temp_version}")
            cur.execute("UPDATE migrations SET current = False WHERE version = %s", (current_version,))
            cur.execute("UPDATE migrations SET current = True WHERE version = %s", (temp_version,))

        # Modify current flag in migrations table
        cur.execute("UPDATE migrations SET current = False WHERE version = %s", (current_version,))
        cur.execute("UPDATE migrations SET current = True WHERE version = %s", (target_version,))
        self.postgres.commit()

    def gen_migration_sql(self, op_type, target_table, field, field_type=None, new_field=None):
        if op_type == "C":
            return self.gen_create_sql(target_table, field, field_type)
        elif op_type == "D":
            return self.gen_delete_sql(target_table, field, field_type)
        elif op_type == "U":
            return self.gen_update_sql(target_table, field, new_field)

    def gen_create_sql(self, table, field, type):
        """ALTER TABLE EmployeeInfoTest ADD COLUMN manager_id VARCHAR"""
        sql = f"ALTER TABLE {table} ADD COLUMN {field} {type}"
        rollback_sql = f"ALTER TABLE {table} DROP COLUMN {field}"

        return sql, rollback_sql

    def gen_delete_sql(self, table, field, type):
        """ALTER TABLE EmployeeInfoTest DROP COLUMN manager_id"""
        sql = f"ALTER TABLE {table} DROP COLUMN {field}"
        rollback_sql = f"ALTER TABLE {table} ADD COLUMN {field} {type}"

        return sql, rollback_sql

    def gen_update_sql(self, table, field, new_field):
        """ALTER TABLE EmployeeInfoTest RENAME COLUMN last_update_date TO update_date"""
        sql = f"ALTER TABLE {table} RENAME COLUMN {field} TO {new_field}"
        rollback_sql = f"ALTER TABLE {table} RENAME COLUMN {new_field} TO {field}"

        return sql, rollback_sql


if __name__ == '__main__':
    # check the target version is which have current flag in the migrations table
    target_table = "EmployeeInfoTest"
    postgre = connect_postgres()
    target_version = 2

    schema_control = SchemaControl(postgre)

    # 1. apply migration
    # field = "manager_id"
    # type = "VARCHAR"
    # schema_control.apply_migrations("C", target_table, field=field, field_type=type)
    # delete test
    # field = "manager_id"
    # type = "VARCHAR"
    # schema_control.apply_migrations("D", target_table, field=field, field_type=type)

    # 2. migration
    schema_control.migration(target_version, target_table)
    print(schema_control.get_current_version(target_table))
