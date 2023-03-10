### Welcome to Dashing DataWarehouse with dbt project! ( ```ongoing``` )

## Dir Structure

```
project
│   README.md
│   requirements.txt  
|   .gitignore  
|   packages.yml
|   dbt_project.yml
│
└───miscellaneous
│   │   profiles.yml     : sample config file
│   
└───models
│   │   schema.yml       : for generic test & documentation
│   └───src              : all core sql file
│   └───dim              : data generated from src
│   └───fact             : data generated from src
│
└───seeds
│   │   *.csv     : static file want to export in DWH
│   
└───snapshots
│   │   *.sql     : history/log of tables
│   
└───tests
│   │   *.sql     : custom test file 
```

# DAG sample
![graph](dbt-dag.png)

# Instruction
+ [Environment setup](#set_environment)
+ [Project setup](#set_project)
+ [Import data to DWH](#import)

# set_environment
+ create python venv
+ install ```requirements.txt```

# set_project
+ DB setup ( ```postgres``` )
    + DBName: dvdrental
    + restore ```miscellaneous --> dvdrental.tar``` in ```public``` schema
    + SchemaName: dashingdvdrental
+ clone the repo
+ goto repo
+ File configure: configure the following file according to ```miscellaneous --> profiles.yml```

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

# import
+ place any ```*.csv``` file in ```seeds``` dir & run ```dbt seed```. A table will be created with the name of csv file.