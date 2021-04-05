import pyodbc
import time
import sys
import pandas as pd
import datetime

# Разделение листа на чанки
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Проерка на номер
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

# Функция сбора логина для MSSQL server
def login(sql_file):
    try:
        with open(sql_file) as f:
            t0 = f.read().split('\n')
            login_sql = t0[0]
            password_sql = t0[1]
    except:
        print('Сбор данных для доступа провалился.')
        time.sleep(60)
        sys.exit()
    return login_sql, password_sql

# Select из базы данных
def select_query(query, login, password, DB='Cursor', isList=False):
    time.sleep(0.5)  # Спим, чтобы не травмировать базу
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=37.203.243.65\CURSORMAIN,49174;'
                          'Database=' + DB + ';'
                                             'UID=' + login + ';'
                                                              'PWD=' + password + ';'
                                                                                  'Trusted_Connection=no;')
    df = pd.read_sql_query(query, conn)
    conn.close()
    if isList == True:
        list_df = df.values
        flat_list = [item for sublist in list_df for item in sublist]
        return flat_list  # Возвращаем лист
    else:
        return df  # Возвращаем Dataframe pandas

# Достаем лист, разделенный запятыми, из .txt файлов
def get_list_from_txt(txt_file, enc='utf-8'):
    try:
        with open(txt_file, encoding=enc) as f:
            t = f.read().split(',')
            list_fin = []
            for t0 in t:
                t1 = t0.replace(' ', '')
                list_fin.append(t1)
    except:
        print('Сбор данных для доступа провалился.')
        time.sleep(60)
        sys.exit()
    return list_fin

# Функция для проксей
def get_proxies(login_sql, password_sql):
    yesterdayTime = datetime.datetime.now() - datetime.timedelta(days=13)
    t0 = yesterdayTime.strftime('%Y%m%d')  # Форматирование даты №2
    proxy_query = "SELECT TOP (1000)[proxy] FROM [CursorImport].[proxy].[Proxys] where dtcreate >= '" + t0 + "' order by dtcreate desc"
    df_proxies = select_query(proxy_query, login_sql, password_sql)
    return df_proxies