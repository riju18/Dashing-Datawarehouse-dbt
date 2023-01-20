### Welcome to Dashing DataWarehouse with dbt project! ( ```ongoing``` )

# Instruction
+ [Environment setup](#set_environment)
+ [Project setup](#set_project)
+ [Export data to DWH](#export)

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

# export
+ place any ```*.csv``` file in ```seeds``` dir & run ```dbt seed```. A table will be created with the name of csv file.