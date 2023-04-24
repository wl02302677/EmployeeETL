from datetime import date
from pydantic import BaseModel
from typing import Optional


# output schema to postgreSQL
class EmployeeInfo:
    """ Employee table """
    employee_id: str
    employee_name: str
    position: str
    joined_date: Optional[date]
    satisfaction_score: Optional[int]
    is_delete: Optional[bool]
    create_date: Optional[date]
    last_update_date: Optional[date]


class SalaryInfo:
    """ Salary table"""
    employee_id: str
    salary_type: str
    salary_currency: Optional[str]
    salary_value: Optional[int]
    create_date: Optional[date]
    last_update_date: Optional[date]


"""
    {
    "id": "abd1234rty",
    "name": "Bob Smith",
    "attributes": {
        "position": "Manager",
        "joinedOn": "2023-02-15T15:09:57.655Z",
        "satisfactionScore": 10.5
    },

    "salaryValues": [
        {
            "type": "Base",
            "currency": "USD",
            "value": 56767
        },
        {
            "type": "Bonus",
            "currency": "USD",
            "value": 5000
        }
    ],
    "isDeleted": false
}
    """
