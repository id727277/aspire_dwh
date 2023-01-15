import pandas as pd
import requests
from sqlalchemy import create_engine
from credentials import CRED_URL

conn = create_engine(CRED_URL)
table_name = 'stg_currency'


def get_table(url):
    html = requests.get(url).content
    return pd.read_html(html)

def strip_links_cites(df):
    for i in df.columns:
        try:
            df[i] = df[i].str.replace(r'\[.*\]', '', regex=True) # removing wiki links
        except AttributeError:
            pass 
    return df

if __name__ == '__main__':

    # getting ISO 4217 codes
    url = "https://en.wikipedia.org/wiki/ISO_4217"
    df_list = get_table(url)
    codes = df_list[1]
    codes.drop(['D[a]', 'Locations listed for this currency[b]', 'Num'], axis=1, inplace=True)
    codes.columns = ['code', 'name_en']

    codes_en = strip_links_cites(codes)

    #getting Russian codes
    url = "https://ru.wikipedia.org/wiki/%D0%9E%D0%B1%D1%89%D0%B5%D1%80%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%B8%D0%B9_%D0%BA%D0%BB%D0%B0%D1%81%D1%81%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%82%D0%BE%D1%80_%D0%B2%D0%B0%D0%BB%D1%8E%D1%82"
    df_list = get_table(url)
    codes = df_list[6]
    codes.columns = codes.columns.get_level_values(0) #removing multi-index
    codes.columns = ['code', 'num', 't', 'name_ru', 'name_en', 'country_rus', 'date']
    codes.drop(['num', 't', 'name_en', 'country_rus', 'date'], axis=1, inplace=True)
    codes.drop([161, 217, 218, 232], axis=0, inplace=True) # dropping rows with no info

    codes_ru = strip_links_cites(codes)



    # merging codes and pushing to STG
    codes = codes_ru.merge(codes_en, on='code')
    print(codes.head())
    codes.to_sql(table_name, con=conn, if_exists='replace', index=False, chunksize=10000)

    # checking 
    query = f"select count(*) as cnt from {table_name}"
    q_get = pd.read_sql(sql=query, con=conn)
    print(f"В таблицу {table_name} загружено {q_get['cnt'][0]} строк.")
