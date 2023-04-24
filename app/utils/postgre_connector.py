import os
import yaml
import psycopg2


def connect_postgres():
    # config_file = './config/db_config.yaml'
    config_file = "C:\\Users\\Sean\\pycharm-project\\code-test-sean-wu\\app\\config\\db_config.yaml"
    if not os.path.exists(config_file):
        raise Exception("There is no configuration file '{}'".format(config_file))
    else:
        with open(config_file) as f:
            config = yaml.safe_load(f)

    user = config["user"]
    pwd = config["pwd"]
    host = config["host"]
    port = config["port"]
    database_name = config["database_name"]

    postgres = psycopg2.connect(
        dbname=database_name,
        user=user,
        password=pwd,
        host=host,
        port=port
    )

    return postgres


# connect_postgres()
