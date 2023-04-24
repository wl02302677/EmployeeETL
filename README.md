# Insurwave Data Engineering (ETL) code test

## Introduction

The HR department of a large corporation wants to start using their operational data to drive insights and improve employee experience. In order to enable that, the data engineering team needs to set up the data pipelines to load the operational data into an analytics database (SQL based) which will be used by data analyst to explore the data and drive insights. 

You can assume the operational data flow to analytics workspace in the form of json files send to an API endpoint for processing. 

With this exercise, you will create a service which given json as an input, inserts the data into a SQL database. 
The example is from the <em>employee</em> data domain. 
- An employee must have a unique identifier, and a name. 
- The HR system also keeps details on the employees salary details. An employee's salary can have multiple constituent parts. Not all employes have salary values. 
- The HR system also keeps track of metadata on different employee attributes. These information is not available for all employees

Below is an example of the json input.

```
{
    "id": "abd1234rty",
    "name": "Bob Smith",
    "attributes": {
        "position": "Manager",
        "joinedOn": "2023-02-15T15:09:57.655Z",
        "satisfactionScoe": 10.5
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
```

The produced solution should:
- build succesfully, 
- run succesfullly to process example json inputs
- contain all the necessary tests (unit tests and integration tests), which should be runable
- have all relevant documentation.

During work on the solution, please use all the good practices that would be normally used during the development of the service.  
Please feel free to use any libraries, patterns or solution structure (including adding projects) that feels right for you.
Please use any SQL RDBMS of your choise. 

For the implementation, please follow the tickets written below.  
For every ticket, please prepare a separate commit/pull-request showing incremental work.


During the interview session, the produced solution will be demoed, reviewed and discussed.  
You will then discuss how additional requirements provided by the interviewer can be implemented


## Ticket-1  Create an API end point for loading data in SQL database 

Please design and create an API endpoint that accepts <em>employee</em> data domain json as input, processes the json and updates the relevant SQL Database tables.  

Please use the sample json provided to generate example data as required for testing the service

- How to use the API

```
http://127.0.0.1:8000/ETLetl/employeeETL?source_info_json={json_format_string}
```

## Ticket-2  Demostrate database schema changes in source control 

Please demostrate how changes to the database schema can be version controled.
For this exercise you can apply any db schema changes of your choise to demostrate the capability. 

- How to use the schema control
1. Use create_migrations_table() to initial the control table
2. When we need to modify, use apply_migrations(self, op_type, target_table, field=None, field_type=None, new_field=None, create_sql=None) to generate migration history
3. Input parameter generating sql to modify schema
4. Use migration() when need to change to specific version, compare target_version with current_version
5. target_version > current_version: goahead_migration(), target_version < current_version: rollback_migration()
6. goahead_migration(): step by step execute sql from current to target 
7. rollback_migration(): step by step execute rollback_sql from current to target

## How to deploy the project?
1. Build the image
```
cd path/code-test-sean-wu/app
docker build . -t <image>:<tag>
EX: docker build . -t wave:0423
```

2. Run the container
```
docker run -d -p <local port>:<container port> <image>:<tag>
EX: docker run -d -p 9090:9090 -p 5432:5432 wave:0423

```

3. check the container
```
docker ps
docker exec -it container-id bash
```

4. Check the API doc page
```
http://127.0.0.1:8080/docs#
```

## Project Control
Jira: https://wl02302677.atlassian.net/jira/software/projects/ICTSW/boards/1

## Documents
https://docs.google.com/document/d/10OUOgbU9Nl9QfMYww3oHRw4ghrMGKYXXZrl5DvhM2GE/edit?usp=sharing