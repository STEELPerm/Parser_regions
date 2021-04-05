import requests
import pandas as pd
import os
import pyodbc
import time
from bs4 import BeautifulSoup
import datetime as dt
import traceback
from docx import Document
import comtypes.client

import parser_utils

def clear_winword_process():
    try:
        os.system("taskkill /f /im WINWORD.EXE")
        time.sleep(15)
    except:
        pass

def find_index_by_str(df, string_to_find, column_index, all_columns=False):
    indexes = []
    for ind, row in df.iterrows():
        if all_columns == True:
            i = 0
            while i < len(df.columns):
                x0 = row[df.columns[i]]
                if string_to_find.lower() in str(x0).lower():
                    indexes.append(ind)
                i -=- int(True)
        else:
            x0 = row[df.columns[column_index]]
            if string_to_find.lower() in str(x0).lower():
                indexes.append(ind)
    return indexes


def FileSaveT():
    clear_winword_process()
    try:
        os.remove('0.docx')
    except:
        pass
    try:
        os.remove('0.rtf')
    except:
        pass
    soup = BeautifulSoup(r.text, 'lxml')
    data_scripts = []
    data = soup.find_all('script', type='text/javascript')  # берем предпоследний скрипт
    for data0 in data:
        data1 = str(data0).replace('            ', '')
        data_split = data1.split('\n')  # Делим на строки
        data_scripts.append(data_split)
        files = []
        urls = []
        for data2 in data_scripts:
            for spl in data2:
                if 'filename' in spl.lower():
                    files.append(spl.lower())
                if ' url:' in spl.lower():
                    urls.append(spl.lower())
        # print(files)
        # print(urls)
        Found = False
        isrtf = False
        for url, file in zip(urls, files):
            if 'протокол' in file.lower():
                url_download = url.split("url: '")[1].replace(' ', '').replace("'", "")
                r_download = requests.get(url_download, headers=headers, verify=False)
                with open('0.docx', 'wb') as f:
                    f.write(r_download.content)
            elif '.rtf' in file.lower():  # бардак с морально устаревшими .rtf файлами
                url_download = url.split("url: '")[1].replace(' ', '').replace("'", "")
                r_download = requests.get(url_download, headers=headers, verify=False)
                with open('0.rtf', 'wb') as f:  # Скачиваем документ
                    f.write(r_download.content)

                wdFormatDOCX = 16  # открываем и пересохраняем как docx
                word = comtypes.client.CreateObject('Word.Application')
                doc = word.Documents.Open(path + '0.rtf')
                doc.SaveAs(path + '0.docx', FileFormat=wdFormatDOCX)
                doc.Close()
                word.Quit()
                isrtf = True
                try:
                    os.remove('0.rtf')
                except:
                    pass

        winners_df = None
        if os.path.isfile('0.docx') == True:
            try:
                document = Document('0.docx')  # Открываем документ и находим в нём таблицы
                tables = []
                for table in document.tables:
                    df = [['' for i in range(len(table.columns))] for j in
                          range(len(table.rows))]
                    for i, row in enumerate(table.rows):
                        for j, cell in enumerate(row.cells):
                            if cell.text:
                                df[i][j] = cell.text
                    tables.append(pd.DataFrame(df))
                winners_df = tables[0]  # Берем первую таблицу и редактируем
                winners_df.columns = winners_df.iloc[0]
                winners_df = winners_df.iloc[1:]
                winners_df = winners_df.reset_index(drop=True)
                if isrtf == True:  # Сохраняет docx он криво, поэтому дополнительно исправляем
                    # winners_df = winners_df.drop([0])
                    ind0 = find_index_by_str(winners_df, 'Порядковый номер заявки', 1)[
                        0]  # Находим строку с порядковым номером и берем индекс
                    winners_df.columns = winners_df.iloc[ind0]  # Подставляем строку с этим индексом в заголовок
                    winners_df = winners_df.iloc[ind0 + 1:]  # Берем все строки ниже этой
                    winners_df = winners_df.loc[:, ~winners_df.columns.duplicated()]
                    winners_df = winners_df.loc[winners_df[winners_df.columns[1]].isin(
                        ['1', '2', '3', '4', '5', '6', '7', '8'])]  # Находим строки, где есть номера
                    winners_df = winners_df[winners_df.columns[1:]]  # Убираем первую "пустую" колонну
                    winners_df = winners_df.reset_index(drop=True)
                    # print(winners_df);time.sleep(30)

                clear_winword_process()
                os.remove('0.docx')
                Found = True
            except:
                print('Не удалось открыть файл протоколов')
                traceback.print_exc()
                clear_winword_process()



path = parser_utils.get_list_from_txt('path.txt')[0]

url = 'https://novobl-zmo.rts-tender.ru/Trade/ViewTrade?id=1862366'
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Content-type': 'application/json; charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0'
}
print(url)

r = requests.get(url, headers=headers, verify=False)


table1 = pd.read_html(r.text)[0]  # Тендер


#print (len(table1))
#print (table1)


for i in range(len(table1)):
    if table1[0][i] == 'Статус':
        tendStatus = table1[1][i]

print(tendStatus)

#print(winners_df[1][1])