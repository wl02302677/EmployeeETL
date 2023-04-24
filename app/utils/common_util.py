import json
from datetime import datetime


def json_to_obj(json_str):
    python_obj = json.loads(json_str)

    return python_obj


def str_to_date(date_str):
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    my_date = datetime.strptime(date_str, date_format)

    return my_date
