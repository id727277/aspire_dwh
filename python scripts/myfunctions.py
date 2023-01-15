import glob
import pandas as pd
import sqlite3
from datetime import datetime
from sqlalchemy import create_engine
from credentials import CRED_URL

conn = create_engine(CRED_URL)

def import_xlsx(path, usecols=None, sheet=0):
    """
    This function reads xlsx file or files from provided path with certain columns (you can specify them) and outputs a combined dataframe.

    Args:
        path (str): a path to an xlsx file
        usecols (str, optional): a str of columns acceptable by pandas pd.read_excel usecols option
        sheet (int, optional): which sheet to read - to pass to the sheet_name arg of pd.read_excel
    Returns:
        pandas DataFrame: dataframe combined from multiple xlsx files.
    """

    files = glob.glob(path)
    df_list = []

    for file in files:
        df = pd.read_excel(file, engine='openpyxl', usecols=usecols, sheet_name=sheet)
        
        df.columns = df.columns.str.lower().str.replace('/','_').str.replace(' ','_')
        # df['loaded_dt'] = datetime.today().replace(microsecond=0).strftime("%Y-%m-%d %H:%M")
        print(f"{file} прочитан, в нем {df.shape[0]} строк.")

        df_list.append(df)
    
    return pd.concat(df_list)

def load_dwh_with_replacement(df, table, layer='stg', schema=None):

    """This function pushes df to a table in db, table will be created automatically. If the table exists, it will be replaced.

    Args:
        df (pandas DataFrame): dataframe to be pushed
        table (str): database table in which to insert data. It will be prefixed with a dwh layer abbreviation, e.g. "stg_".
        schema (str, optional): database schema. Defaults to None.
        layer (str, optional): layer of the dwh to use in table name. Defaults to 'stg'.
    """

    table_to = '_'.join([layer,table])
    df.to_sql(table_to,con=conn, if_exists='replace', index=False, chunksize=10000)

    query = f"select count(*) as cnt from {table_to};"
    resp = pd.read_sql(query,con=conn)
    print(f"В таблицу {table_to} загружено {resp['cnt'][0]} строк.")

def execute_sql_script(file):
    """This function executes sql script.

    Args:
        file (str): path to the sql script to execute.
    """

    with sqlite3.connect('dwh.db') as con:
        cursor = con.cursor()

        with open(file, 'r') as sql_file:
            sql_script = sql_file.read()

        cursor.executescript(sql_script)
        print(f"Скрипт {file} выполнен")

