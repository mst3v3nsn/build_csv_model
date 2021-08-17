from colorama import Fore, Style
from datetime import datetime, timedelta
import os

from .database import get_db_engine


def range_dt(dt, minimum=-1.0, maximum=1.0):
    """Calculation of time range

    :param str dt: Date time string
    :param float minimum: Minimum threshold of timespan
    :param float maximum: Maximum threshold of timespan

    :return: Start string, End String
    """

    start = dt + timedelta(hours=minimum) - timedelta(minutes=10)
    end = dt + timedelta(hours=maximum)

    return start, end


def time_add_time(_time, span, multiplier):
    """Add ten min to timestep

    :param timedelta _time: Timestep delta
    :param float span: Amount of time need for data in hours
    :param int multiplier: Amount to multiply

    :return: Timedelta for time at increment
    :rtype timedelta
    """

    _nt = (_time - timedelta(hours=span)) + (timedelta(minutes=10) * multiplier)

    return _nt


def convert_date(_x):
    """Extract and reformat datetime string

    :param str _x: Date string to convert

    :return: Date string
    :rtype: datetime
    """

    date = datetime.strptime(_x, '%Y-%m-%d').date()

    return date


def convert_time(y):
    """Convert time string and reformat

    :param str y: Time string to convert

    :return: Time string
    :rtype: datetime
    """

    time = datetime.strptime(y, '%H:%M:%S').time()

    return time


def val_range(t):
    """Calculation of next ten minute increment of time

    :param timedelta t: Timedelta to add increment to

    :return: Start datetime, End datetime
    """

    start = t - timedelta(minutes=10)
    end = t

    return start, end


def calc_incs(_span):
    """Calculate the increments needed for timesteps within table

    :param flot _span: Span of time needed

    :return: Range needed to complete timesteps
    :rtype: int
    """

    return int(_span*60/10)


def path_inc(b, filename):
    """Increments outputs of filename in directory specified

    :param str b: Output path
    :param str filename: Name of file

    :return:Absolute path for file to be saved at execution
    :rtype: str
    """

    # store target file into local variable
    f_name = filename

    # if file exists
    if os.path.exists(os.path.join(b, f_name)):

        # remove extension from filename
        f_name_new = os.path.splitext(f_name)[0]

        # split off delimiter (if exists)
        ret = f_name_new.rsplit('.', 1)

        # no delimiter exists
        if len(ret) == 1:

            # add '.1' to filename and recursively check new name entry
            return path_inc(b, f_name_new + '.' + str(1) + '.csv')

        # a delimiter exists
        else:
            return path_inc(b, ret[0] + '.' + str(int(ret[-1]) + 1) + '.csv')

    # no file present, use original target
    else:

        filex = os.path.join(b, f_name)

        # return checked filename
        return filex


def time_calc(ttime):
    """Calculate time elapsed in (hours, mins, secs)

    :param float ttime: Amount of time elapsed

    :return: int, int, float
    """

    # create local copy
    tt = ttime

    # if not divisible by 60 (minutes), check for hours, mins, secs
    if divmod(tt, 60)[0] >= 1:
        h_t = divmod(tt, 60 * 60)[0]
        tt = tt - (h_t * 60 * 60)
        m_t = divmod(tt, 60)[0]
        tt = tt - (m_t * 60)
        s_t = tt

        return h_t, m_t, s_t

    # only return seconds
    else:
        h_t = 0
        m_t = 0
        s_t = tt

        return h_t, m_t, s_t


def default_model(path):
    """Returns default output directory for models

    :param str path: Default path of main file

    :returns: Default path of output directory
    :rtype: str
    """

    m_path = os.path.join(path, 'modeling', 'output_models')

    print(f'{Fore.GREEN}'
          f'\nNo model output directory specified, using default: '
          f'{Fore.LIGHTGREEN_EX}{m_path}'
          f'{Style.RESET_ALL}')

    return m_path


def default_query(path):
    """Returns default output directory

    :param str path: Default path of main file

    :returns: Default path of output directory
    :rtype: str
    """

    d_path = os.path.join(path, 'modeling', 'saved_queries')

    print(f'{Fore.GREEN}'
          f'\nNo query output directory specified, using default: '
          f'{Fore.LIGHTGREEN_EX}{d_path}'
          f'{Style.RESET_ALL}')

    return d_path


def check_output_dirs(m_path, q_path, m, q):
    """Output directory checker caller function

    :param str m_path: Output directory for model path
    :param str q_path: Output directory for query path
    :param str m: Name type for model output
    :param str q: Name type for query output
    """

    check_dir(m_path, m, 'model')
    check_dir(q_path, q, 'query')


def check_dir(path, defaults, type_path):
    """Check for output directory and create if does not exist

    :param str path: Default path of main file
    :param bool defaults: Tracks whether the output directory is default
    :param str type_path: Name type of output directory
    """

    temp_dir, _ = os.path.split(path)
    if not os.path.isdir(temp_dir):
        os.mkdir(temp_dir)
    if not os.path.isdir(path):
        os.mkdir(path)
    if not defaults:
        print(f'{Fore.GREEN}'
              f'\nOutput directory specified for {type_path}: '
              f'{Fore.LIGHTGREEN_EX}{path}'
              f'{Style.RESET_ALL}')


def test_sql_details(server, table):
    """Test SQL details caller function

    :param str server: IP address of SQL server
    :param str table: Name of table in SQL database
    """

    test_sql_connection(server)
    test_sql_table(table)


def test_sql_connection(svr):
    """Test SQL database connection

    :param str svr: IP address of SQL serve
    """

    try:
        engine = get_db_engine()
        conn_c = engine.connect()
        print(f'{Fore.GREEN}'
              f'SQL Engine connected to '
              f'{Fore.LIGHTGREEN_EX}{svr}{Fore.GREEN}!'
              f'{Style.RESET_ALL}')
        conn_c.close()
    except Exception as e:
        print(e)
        print(f'{Fore.RED}'
              f'\nSQL Engine not able to connect! '
              f'Please verify settings in \'config.py\' file or that your SQL driver is installed correctly.'
              f'{Style.RESET_ALL}\r\n')
        exit(1)


def test_sql_table(table):
    """Test SQL table exists

    :param str table: Name of table in SQL database
    """

    try:
        engine = get_db_engine()
        conn_c = engine.connect()
        result = engine.dialect.has_table(conn_c, table)

        if not result:
            conn_c.close()
            raise Exception
        else:
            conn_c.close()

    except Exception:
        print(f'{Fore.RED}'
              f'\nThe table, {Fore.LIGHTRED_EX}{table}{Fore.RED}, does not exist!'
              f'{Style.RESET_ALL}')
        exit(1)


def test_date_and_time(d_str, t_str):
    """Test date and time details caller function

    :param str d_str: Date string
    :param str t_str: Time string
    """

    test_date(d_str)
    test_time(t_str)


def test_date(date_string):
    """Test date string

    :param str date_string: Date string
    """

    try:
        datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")
        exit(1)


def test_time(time_string):
    """Test date string

    :param str time_string: Time string
    """

    try:
        datetime.strptime(time_string, '%H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be HH:MM:SS")
        exit(1)
