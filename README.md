# MillionSongDataset_ETL

## This project is about to extract data from Million Song Dataset (Subset-1.8GB) and put them in My-SQL local database.
### Main files:
      1 ETL_FINAL.ipynb  -> contains all necessary etl steps needed to extract data from .h5 files and .json (log) files.
      2 sql_queries.py   -> contains necessary sql queries for creating local database, tables, data insertion
      3 create_tables.py -> contain a python script that utilizes 'sql_queries.py' file that first creates song database then tables.
      4 etl.py           -> final script file that extract data from .h5, .json files and put them in mysql local database (that is created
                            using 'create_tables.py')

## How to run project:
#### First you need to run create_tables.py file, open terminal(or cmd) and type:
     python -W ignore create_tables.py
#### Next run etl.py by typing:
    python -w ignore etl.py

### I highly recommend to first have a brief look at ETL_FINAL.ipynb to understand etl process!
                            
