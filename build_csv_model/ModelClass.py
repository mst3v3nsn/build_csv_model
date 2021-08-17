from colorama import Fore, Style
import pandas as pd
import sqlalchemy as sa
import os

from .helper import range_dt, time_add_time, calc_incs, path_inc
from .database import get_db_engine
from .SubsetClass import SubsetClass


# default SQL driver (Windows)
driver = 'SQL SERVER'
linux = False
os_name = 'Windows'

# if Linux
if os.name != 'nt':
    linux = True
    driver = 'ODBC Driver 17 for SQL Server'
    os_name = 'Linux'


class ModelClass(object):
    """
    Class that stores details of a model to be created
    """
    def __init__(self, date_time, time_span, table, column_index, column_name):
        """
        Constructor for ModelClass

        :param datetime date_time: Datetime string for query
        :param float time_span: Amount of time to span based on original datetime string
        :param str table: Name of table in database
        :param str column_index: Name of column to use as an index '_TIMESTAMP'
        :param str column_name: Column name of tags in database
        """

        self.date_time = date_time
        self.time_span = time_span
        self.table = table
        self.column_index = column_index
        self.column_name = column_name
        self.model_df = None
        self.query_df = None
        self.model_output_file = ''
        self.query_output_file = ''
        self.subset_list = []

        metadata = sa.MetaData()

        self.data_table = sa.Table(self.table,
                                   metadata,
                                   sa.Column('id', sa.INTEGER),
                                   sa.Column('_NAME', sa.VARCHAR),
                                   sa.Column('_NUMERICID', sa.INTEGER),
                                   sa.Column('_VALUE', sa.VARCHAR),
                                   sa.Column('_TIMESTAMP', sa.DATETIME),
                                   sa.Column('_QUALITY', sa.INTEGER))

        # create datetime string
        self._start, self._end = range_dt(self.date_time, minimum=-self.time_span, maximum=0)

        # calculate increments
        _incs = calc_incs(self.time_span)

        # initialize array for timestep increments
        self.min_increments = [None] * (_incs + 1)

        # fill array with equal timesteps
        for x in range(0, _incs + 1):
            self.min_increments[x] = time_add_time(self._end, time_span, x)

    def init_model_df(self):
        """
        Initialize model dataframe to store calculations per tag and timestep
        """

        self.model_df = pd.DataFrame(columns=self.query_df[self.column_name].unique())

        # add _TIMESTAMP column to dataframe
        self.model_df[self.column_index] = self.min_increments

        # set row index to _TIMESTAMP
        self.model_df.set_index(self.column_index, inplace=True)

    def create_query_df(self):
        """
        Query database between calculated timeframe
        """

        # display output message for timeframe
        print(
            f'{Fore.GREEN}\nQuerying database for tags between the timeframe: '
            f'{Fore.LIGHTGREEN_EX}{str(self._start)}{Fore.GREEN} and {Fore.LIGHTGREEN_EX}{str(self._end)}'
            f'{Style.RESET_ALL}')
        print(
            f'{Fore.GREEN}\nTIMESPAN: '
            f'{Fore.LIGHTGREEN_EX}{self.time_span} hours'
            f'{Style.RESET_ALL}')

        engine = get_db_engine()
        offset = 0
        chunk_size = 100000

        dfs = []
        while True:
            sa_select = sa.select(
                [self.data_table],
                whereclause=sa.and_(
                    self.data_table.c._TIMESTAMP > '{}'.format(self._start),
                    self.data_table.c._TIMESTAMP <= '{}'.format(self._end)),
                limit=chunk_size,
                offset=offset,
                order_by=self.data_table.c._NUMERICID
            )
            dfs.append(pd.read_sql(sa_select, engine))
            offset += chunk_size
            if len(dfs[-1]) < chunk_size:
                break

        self.query_df = pd.concat(dfs)

    def get_guery_df(self):
        """
        Get query dataframe

        :return: Dataframe from query of SQL database
        """

        return self.query_df

    def get_model_df(self):
        """
        Get model dataframe

        :return: Dataframe for model
        """

        return self.model_df

    def get_min_increments(self):
        """
        Get array of timestep increments

        :return: Array of timestep increments
        :rtype: array
        """

        return self.min_increments

    def set_model_output(self, path):
        """
        Set output path and file for model

        :param str path: Path for output directory of model
        """

        file = f'model_R{str(self.time_span).replace(".", "_")} ({str(self.date_time).replace(":","_")}).csv'
        self.model_output_file = path_inc(path, file)

    def get_model_output(self):
        """
        Get output file path for model file

        :return: Output file path for model
        :rtype: str
        """

        return self.model_output_file

    def set_query_output(self, path):
        """
        Set output path and file for query

        :param str path: Path for output directory of query
        """

        file = f'sql_query_R{str(self.time_span).replace(".", "_")} ({str(self.date_time).replace(":","_")}).csv'
        self.query_output_file = path_inc(path, file)

    def get_query_output(self):
        """
        Get output path for query file

        :return: Output file path for query
        :rtype: str
        """

        return self.query_output_file

    def create_query_csv(self):
        """
        Create csv for query and output to directory specified
        """

        self.query_df.to_csv(self.query_output_file)

    def create_model_csv(self):
        """
        Create csv for model and output to directory specified
        """

        self.model_df.to_csv(self.model_output_file)

    def create_subset_list(self):
        """
        Create list of subset objects that store details of each timestep block
        """

        row = 0
        for time_step in self.min_increments:
            subset = SubsetClass(time_step=time_step, query_df=self.query_df, model_df=self.model_df, row=row)
            self.subset_list.append(subset)
            row += 1

    def get_subset_list(self):
        """
        Get list of SubsetClass objects

        :return: List of SubsetClass objects
        :rtype: list
        """

        return self.subset_list

    def wait_for_threads_of_subclass(self):
        """
        Wait for threads of subclass to finish running
        """

        running = True

        while running:
            status_list = []
            for subset in self.subset_list:
                if subset.get_thread_status():
                    status_list.append(True)
                else:
                    status_list.append(False)
            if not status_list.__contains__(True):
                running = False

    def set_model_df_at_time_step(self):
        """
        Set model dataframe at timestep to subset dataframe for entire row
        """

        for subset in self.subset_list:
            ts = subset.get_time_step()
            self.model_df.loc[ts] = subset.get_model_df().loc[ts]
