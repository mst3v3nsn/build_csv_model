from sqlalchemy import create_engine
import base64
import os

from .config import db, user, enc_psswd, server

driver = 'SQL SERVER'

# if Linux
if os.name != 'nt':
    driver = 'ODBC Driver 17 for SQL Server'

conn_string = f'mssql+pyodbc://{user}:{base64.b64decode(enc_psswd.encode("ascii")).decode("ascii")}@{server}/{db}?driver={driver}'
engine = create_engine(conn_string, fast_executemany=True)


def get_db_engine():
    """
    Get database engine

    :return: Return engine
    """

    return engine
