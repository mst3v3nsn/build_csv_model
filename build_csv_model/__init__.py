from datetime import datetime
from pathlib import WindowsPath, PosixPath
from colorama import Fore, Style
import pandas as pd
import time
import os

from .ModelClass import ModelClass

# import config.py variables
from .config import db, server, user, _table, column_index, _sample_date, _sample_time, _time_span, column_name

# import helper functions from helper.py
from .helper import test_sql_details, test_date_and_time, default_model, default_query, check_output_dirs, convert_time, time_calc, convert_date

# default SQL driver (Windows)
driver = 'SQL SERVER'
linux = False
os_name = 'Windows'
default_path = ''

# if Linux
if os.name != 'nt':
    linux = True
    driver = 'ODBC Driver 17 for SQL Server'
    os_name = 'Linux'
    default_path = PosixPath.home()
else:
    default_path = WindowsPath.home()

# suppress pandas splice copy warning
pd.options.mode.chained_assignment = None


def create_model(sample_date=_sample_date,
                 sample_time=_sample_time,
                 table=_table,
                 time_span=_time_span,
                 tag_name=column_name,
                 model_path=None,
                 query_path=None):
    """Create CSV model from database

    :param str sample_date: Date of sample YYYY-MM-DD
    :param str sample_time: Time of sample HH:MM:SS
    :param str table: Name of target table in database
    :param str time_span: Length of time needed for data in hours
    :param str tag_name: Column name for tags
    :param str model_path: Output directory for CSV model
    :param str query_path: Output directory for CSV query
    """

    # display SQL connection details
    print(
        f'{Fore.CYAN}\nCONNECTION DETAILS:{Style.RESET_ALL}{Fore.LIGHTWHITE_EX}'
        f'\n\tSERVER: {server}'
        f'\n\tDRIVER: {driver}'        
        f'\n\tDB: {db}'
        f'\n\tUSER: {user}'       
        f'\n{Style.RESET_ALL}')

    # display OS name
    print(f'{Fore.GREEN}Running on {Fore.LIGHTWHITE_EX}{os_name}{Style.RESET_ALL}\r\n')

    # test sql connection and table
    test_sql_details(server, table)

    # test sample date and time
    test_date_and_time(sample_date, sample_time)

    default_m = False
    default_q = False

    # use default if no directories specified
    if model_path is None:
        model_path = default_model(default_path)
        default_m = True
    if query_path is None:
        query_path = default_query(default_path)
        default_q = True

    # check path of model and query output directories
    check_output_dirs(model_path, query_path, default_m, default_q)

    # start timer
    start_time = time.time()

    # create datetime string
    _dt = datetime.combine(convert_date(sample_date), convert_time(sample_time))

    model = ModelClass(date_time=_dt, time_span=time_span, table=table, column_index=column_index, column_name=tag_name)

    model.set_model_output(model_path)
    model.set_query_output(query_path)
    model.create_query_df()
    model.init_model_df()

    # display output for file save
    print(
        f'{Fore.LIGHTGREEN_EX}'
        f'\nSQL Query Saved: {Fore.YELLOW}{model.get_query_output()}'
        f'{Style.RESET_ALL}')

    model.create_query_csv()

    # display output for size of dataframe
    print(
        f'{Fore.LIGHTGREEN_EX}'
        f'\tBase Dataframe Created with {Fore.YELLOW}{len(model.get_model_df().columns)} '
        f'{Fore.LIGHTGREEN_EX}columns.{Fore.LIGHTGREEN_EX}'
        f'{Style.RESET_ALL}')

    model.create_subset_list()
    model.wait_for_threads_of_subclass()
    model.set_model_df_at_time_step()

    # display output for file save
    print(
        f'{Fore.LIGHTGREEN_EX}'
        f'\nOutput Model Saved: {Fore.YELLOW}{model.get_model_output()}'
        f'{Style.RESET_ALL}')

    model.create_model_csv()

    # display time elapsed
    end_time = time.time()
    hours_t, min_t, sec_t = time_calc(end_time - start_time)

    print(
        f'{Fore.LIGHTBLUE_EX}'
        f'\nTime Elapsed: {Fore.LIGHTMAGENTA_EX}{hours_t} hours {min_t} minutes {sec_t} seconds.'
        f'{Style.RESET_ALL}')


# MAIN
if __name__ == '__main__':
    """Main Loop"""
    create_model()
