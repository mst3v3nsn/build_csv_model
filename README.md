# build_csv_model
- This package was written to build csv models based on data from SQL table queries returning large datasets

#### Linux/Windows compatible 
- The correct SQL driver is installed (ODBC Driver - Version 17)
- Linux: [Microsoft SQL ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15)
- Windows: [Microsoft SQL ODBC Driver](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15)

#### To clone this repository:
```
git clone https://github.com/mst3v3nsn/build_csv_model.git
```

#### Ensure python is installed
```
python --version
```

##### After driver installed, install the package using the command:
```
pip install 'path\to\setup.py'
```

##### To uninstall package:
```
pip uninstall build_csv_model
```

#### Usage:
```python
from build_csv_model import create_model

create_model() # uses default values specified within config.py file

```
#### Parameters of create_model: (overrides values specified in config.py)
- sample_date: Date of sample YYYY-MM-DD
- sample_time: Time of sample HH:MM:SS
- table: Name of target table in database
- time_span: Length of time needed for data in hours
- tag_name: Column name for tags in database
- model_path: Output directory for CSV model
- query_path: Output directory for CSV query

##### Caveats:
- Uses threading to build each block (timestep) of the model dataframe in parallel. From what I researched, this was the best approach for my use case to acheive desired results and optimizations.
- Uses chunking to speed-up database querying of large datasets via [SQLAlchemy](https://docs.sqlalchemy.org/en/14/).
- Uses pandas to process and manipulate returned data utilizing dataframes.
- For each point in the model created, the average of values at each timestep is taken. For boolean values of a point in the model, if any True is found the resultant defaults to True. 
