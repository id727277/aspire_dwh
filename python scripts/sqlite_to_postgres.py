
import pandas as pd
from credentials import CRED_URL, CRED_URL_PG
from sqlalchemy import create_engine

sqlt = create_engine(CRED_URL)
pg = create_engine(CRED_URL_PG)

tables = ['nda1', 'nda2', 'nda3', 'nda4', 'nda5']

def sqlite_to_pg(eng_from, eng_to, tables):
    for table in tables:
        query = f"select * from {table};"
        df = pd.read_sql(query,con=eng_from)
        df.to_sql(table,con=eng_to, schema='public',if_exists='replace', index=False)
        
        query = f"select count(*) as cnt from {table};"
        resp = pd.read_sql(query,con=eng_to)
        print(f"В таблицу {table} загружено {resp['cnt'][0]} строк.")


if __name__ == '__main__':
    
    sqlite_to_pg(sqlt,pg,tables)