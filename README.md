### Welcome to Dashing DataWarehouse with dbt project! ( ```ongoing``` )

# Instruction
+ [Environment setup](#set_environment)
+ [Project setup](#set_project)


# set_environment
+ create python venv
+ install ```requirements.txt```

# set_project
+ DB setup ( ```postgres``` )
    + DBName: dvdrental
    + SchemaName: dashingdvdrental
+ clone the repo
+ goto repo
+ File configure: configure the following file according to ```miscellaneous -- > profiles.yml```

    + Windows
        + C:\Users\This Pc\\.dbt
    + linux
    + mac
+ check config: ```dbt debug```
+ test project: ```dbt test```
+ run project: ```dbt run```
+ snapshot/log generate: ```dbt snapshot```
+ Documentation of project:
    ```text
    dbt docs generate
    ```
    ```text
    dbt docs serve
    ```