from colorama import Fore, Style
from threading import Thread

from .helper import val_range
from .config import column_index, column_name, debug


class SubsetClass(object):
    """
    Class that stores SubsetClass details for components of model
    """
    def __init__(self, time_step, query_df, model_df, row):
        """
        SubsetClass constructor

        :param str time_step: Timestep of model
        :param dataframe query_df: Subset dataframe from query
        :param dataframe model_df: Subset dataframe from model
        :param int row: Row of model dataframe
        """

        self.time_step = time_step
        self.start, self.end = val_range(self.time_step)
        self.subset_df = query_df.query(f'\"{self.start}\" < {column_index} <= \"{self.end}\"')
        self.timestep_model_df = model_df.query(f'{column_index} == \"{self.time_step}\"')
        self.row = row
        self.thread = Thread(target=self.fill_model_df_row)
        self.thread_finished = False

        self.thread.start()

    def get_thread_status(self):
        """
        Checks if thread is still running

        :return: T/F if thread is running
        :rtype: bool
        """


        if not self.thread.is_alive() and not self.thread_finished:
            if debug:
                print(f'{Fore.LIGHTYELLOW_EX} Thread: {str(self.thread.ident)} finished!')
            self.thread_finished = True

        return self.thread.is_alive()

    def get_thread(self):
        """
        Get thread from SubsetClass object

        :return: Thread object
        :rtype: thread
        """

        return self.thread

    def get_model_df(self):
        """
        Get subset model dataframe

        :return: Subset model dataframe
        :rtype: dataframe
        """

        return self.timestep_model_df

    def get_time_step(self):
        """
        Get timestep of SubsetClass object

        :return: Timestep string
        :rtype: str
        """

        return self.time_step

    def fill_model_df_row(self):
        """
        Fill timestep dataframe
        """

        if debug:
            # output display message to keep track of row
            print(
                f'{Fore.LIGHTYELLOW_EX}'
                f'\n_TIMESTEP: {str(self.time_step)} (ROW: {self.row}) thread started!'
                f'{Style.RESET_ALL}')

        # variable to keep track of column index
        c_idx = 0

        # loop through elements in subset dataframe (tags)
        for xitem in self.timestep_model_df:

            vals_df = self.subset_df.query(f'{column_name} == \"{xitem}\"')

            if len(vals_df) == 0:
                # return None
                continue

            # if dataframe is not empty, calculate point
            if len(vals_df) > 0:

                # zero ending value variable
                xval = 0

                # loop through dataframe of values for (tag)
                for _idx, row in vals_df.iterrows():

                    # check for 0 or 1
                    if row['_VALUE'] == '1' or row['_VALUE'] == '0':

                        # return 1 if exists a 1 within data for tag
                        if row['_VALUE'] == '1':

                            xval = 1

                            break

                        # keep adding zeros
                        else:
                            xval = int(row['_VALUE'])

                    # keep adding values (floats)
                    else:
                        xval += float(row['_VALUE'])

                if not xval == 1 and not xval == 0:
                    # take the average of float values
                    value = format(xval / len(vals_df), '.5g')

                    if debug:
                        # display output messages when filling point into dataframe
                        print(
                            f'{Fore.LIGHTCYAN_EX}'
                            f'\n\tFilled point: ({str(self.time_step)}) X ({xitem}) with: '
                            f'{Fore.LIGHTMAGENTA_EX} (ROW: {self.row})(COL {c_idx}): '
                            f'{Fore.LIGHTWHITE_EX}{value}'
                            f'{Style.RESET_ALL}')

                    # set average into dataframe
                    self.timestep_model_df.loc[self.time_step, xitem] = value

                elif xval == 0 or xval == 1:

                    # take the average of float values
                    value = xval

                    if isinstance(value, int):
                        value = format(value, '.0f')
                    if isinstance(value, float):
                        value = format(value, '.1f')

                    if debug:
                        # display output messages when filling point into dataframe
                        print(
                            f'{Fore.LIGHTCYAN_EX}'
                            f'\n\tFilled point: ({str(self.time_step)}) X ({xitem}) with: '
                            f'{Fore.LIGHTMAGENTA_EX} (ROW: {self.row})(COL {c_idx}): '
                            f'{Fore.LIGHTWHITE_EX}{value}'
                            f'{Style.RESET_ALL}')

                    # set average to dataframe entry
                    self.timestep_model_df.loc[self.time_step, xitem] = value

                else:

                    continue

            # update column index
            c_idx += 1
