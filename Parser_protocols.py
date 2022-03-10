import requests
import pandas as pd
import os
import sys
import pyodbc
import time
from bs4 import BeautifulSoup
import datetime as dt
import traceback
from docx import Document
import comtypes.client
import re
#import win32com.client
# lxml
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

import parser_utils

def clear_winword_process():
    try:
        os.system("taskkill /f /im WINWORD.EXE")
        time.sleep(5)
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

# Привязка ИНН и КПП на импорте, согласно оргу, полученному на сайте
def find_org_in_base(org_string, login_sql, password_sql):  # Функция необходимая, но очень медленная
    org_id = None
    inn0 = None
    kpp0 = None
    # Чистим string
    org_string = org_string.replace('»', '"').replace('«', '"').replace("'", '"')
    string_edited = org_string.replace('  ', ' ').replace('- ', '-').replace(' -.', '').replace(' -', '-').replace(' - ', '-')\
        .replace('" ', '"')
    string_edited2 = string_edited.replace('"', '')
    string_edited3 = string_edited.replace('общество с ограниченной ответственностью ', '').replace('огуп ', 'областное государственное унитарное предприятие ').replace('общество с огранниченной ответственностью ', '')

    string_edited3_1 = string_edited.replace('общество с ограниченной ответственностью', '')
    string_edited4 = string_edited.replace('общество с ограниченной ответственностью ', '').replace('общество с огранниченной ответственностью ', '').replace('"', '')
    string_edited5 = string_edited.replace('открытое акционерное общество ', '').replace('"', '')
    string_edited6 = string_edited.replace('закрытое акционерное общество ', '').replace('"', '')
    string_edited7 = string_edited.replace('казенное ', '').replace('"', '')
    string_edited8 = string_edited.replace('акционерное общество ', '').replace('"', '')
    string_edited9 = string_edited.replace(' ч.', '').replace('ч', '')
    string_edited10 = string_edited.replace('ооо ', '').replace('ООО ', '').replace('оoo', '').replace('ooo','').replace('ЗАО ', '').replace('зао ', '')
    string_edited11 = string_edited.replace(' ооо', '').replace(' ООО', '')

    # print(org_string, string_edited, string_edited2, string_edited3)
    # Этот метод хуже для базы, но в разы быстрее итерации через список
    org_query2 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + org_string + "%' or OrgNmS like '%" + org_string + "%' order by isnull(isZakupki,0) desc"
    df_org2 = parser_utils.select_query(org_query2, login_sql, password_sql)

    if df_org2.empty == True:
        org_query2_1 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited + "%' or OrgNmS like '%" + string_edited + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query2_1, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем по второму едиту
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited2 + "%' or OrgNmS like '%" + string_edited2 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3 + "%' or OrgNmS like '%" + string_edited3 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query4, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3.replace(' ', '') + "%' or OrgNmS like '%" + string_edited3 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры и кавычек
        #print('Org:'+string_edited4+'End')
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited4 + "')+'%' or OrgNmS like '%" + string_edited4 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры ОАО
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited5 + "')+'%' or OrgNmS like '%" + string_edited5 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры ЗАО
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited6 + "')+'%' or OrgNmS like '%" + string_edited6 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания "казенное"
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + string_edited7 + "%' or OrgNmS like '%" + string_edited7 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited.replace('ё', 'е') + "%' or OrgNmS like '%" + string_edited.replace('ё', 'е') + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query4, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры АО
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where replace(orgNm,'\"','') like '%" + "'+RTRIM('"+string_edited8 + "')+'%' or OrgNmS like '%" + string_edited8 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем по сокращенному имени, если это ИП
        org_query4 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNmSS like '%" + string_edited9.replace('ё', 'е') + "%' or OrgNmS like '%" + string_edited9.replace('ё', 'е') + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query4, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited10 + "%' or OrgNmS like '%" + string_edited10 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited3_1 + "%' or OrgNmS like '%" + string_edited3_1 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == True:  # Ищем после отбрасывания орг структуры
        org_query3 = "SELECT Org_ID, INN, KPP from [Cursor].[dbo].Org where OrgNm like '%" + string_edited11 + "%' or OrgNmS like '%" + string_edited11 + "%' order by isnull(isZakupki,0) desc"
        df_org2 = parser_utils.select_query(org_query3, login_sql, password_sql)

    if df_org2.empty == False:  # Если нашли - заменяем None
        print(org_string)
        org_id = int(df_org2['Org_ID'][0])
        inn0 = df_org2['INN'][0]
        kpp0 = df_org2['KPP'][0]

    return org_id, inn0, kpp0

def insert_prots_query(notif_id0, notif0, why_not0, main_stat0, prot_num0, orgnm, org_id0, org_price, org_dec, login_sql, password_sql, Debug=True):
    if prot_num0 == 0 or prot_num0 == 1:
        org_query = "[Winner], [Winner_ID], [Winner_Price], [Winner_Decision]"
    elif prot_num0 == 2:
        org_query = "[SecondWinner], [SecondWinner_ID], [SecondWinner_Price], [SecondWinner_Decision]"
    elif prot_num0 == 3:
        org_query = "[ThirdWinner], [ThirdWinner_ID], [ThirdWinner_Price], [ThirdWinner_Decision]"
    elif prot_num0 == 4:
        org_query = "[FourthWinner], [FourthWinner_ID], [FourthWinner_Price], [FourthWinner_Decision]"
    else:
        org_query = "[Winner], [Winner_ID], [Winner_Price], [Winner_Decision]"
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=37.203.243.65\CURSORMAIN,49174;'
                          'Database=CursorImport;'
                          'UID='+login_sql+';'
                          'PWD='+password_sql+';'
                          'Trusted_Connection=no;')
    cursor = conn.cursor()
    cursor.execute(
        "insert into [CursorImport].[import].[Regional_Protocols]([Notif_id], [NotifNr], [why_not], [main_stat], [prot_num], [create_dt], "+org_query+")"
        "values (?, ?, ?, ?, ?, getdate(), ?, ?, ?, ?) ",
        notif_id0, notif0, why_not0, main_stat0, prot_num0,
        orgnm, org_id0, org_price, org_dec
    )
    if Debug == False:
        conn.commit()
    conn.close()

def price_cleaner(price_string):

    # STEEL от 05.03.2022 чтобы копейки не обрезались
    price_fin = price_string.lower().split(' руб')[0].replace(' ', '').replace(',', '.')

    if price_fin.count('.') >= 2:
        price_fin = price_fin.split('.')[0]

    if parser_utils.is_number(price_fin) == False:
        price_fin = price_string.split(' (')[0].replace(' ', '').split(',')[0]
        if parser_utils.is_number(price_fin) == False:
            price_fin = None

    # price_fin = price_string.lower().split(' руб')[0].replace(' ','').split(',')[0]
    # if parser_utils.is_number(price_fin) == False:
    #     price_fin = price_string.split(' (')[0].replace(' ','').split(',')[0]
    #     if parser_utils.is_number(price_fin) == False:
    #         price_fin = None
    return price_fin

def get_protocols(login_sql, password_sql, isDebug=True, isProxy=True, args=None):
    path = parser_utils.get_list_from_txt('path.txt')[0]

    if isProxy == True:
        df_proxy = parser_utils.get_proxies(login_sql, password_sql)
        print(df_proxy.sample(1))
    if args == None:
        reg_sites_query = "SELECT id FROM [CursorImport].[import].[RegionalWebsites] where Host = 'zmo-new-webapi.rts-tender.ru'" # where Host = 'zmo-new-webapi.rts-tender.ru'
    else:
        reg_sites_query = "SELECT TOP(1) id FROM [CursorImport].[import].[RegionalWebsites] where Host = 'zmo-new-webapi.rts-tender.ru'"
    df_reg_site = parser_utils.select_query(reg_sites_query, login_sql, password_sql)

    for df_id, reg_site_id in df_reg_site.iterrows():
        reg_site_id0 = str(reg_site_id['id'])
        isnotdone = True
        temp_done = []

        # while isnotdone == True:
        # try:
        if isProxy == True:
            proxy = df_proxy.sample(1)['proxy'].reset_index(drop=True)[0]  # Берем один рандомный прокси из DataFrame
            print('Подключаюсь, используя прокси: ' + str(proxy))
            prox = str(proxy).replace(' ', '')
            proxies = {'http': 'http://' + prox, 'https': 'http://' + prox, }
        else:
            print('Подключаюсь, без прокси')


        if args == None:
            # запрос из текстового файла
            with open('query_protocols.txt', encoding='cp1251') as f:
                t = f.read().split('\r\n')
                ' '.join(t)
                df_tenders_query1 = t[0].replace('\n', ' ')
                print(df_tenders_query1)
                print(reg_site_id0)

                if "where" in df_tenders_query1:
                    df_tenders_query1 = df_tenders_query1 + " and RegSite_ID=" + reg_site_id0
                else:
                    df_tenders_query1 = df_tenders_query1 + " where RegSite_ID=" + reg_site_id0
            # df_tenders_query1 = "SELECT rc.* FROM [CursorImport].[import].[RegionalCommon] rc join [CursorImport].[import].Regional_Notif rn on rc.Local_Notif_ID=rn.NotifNr join [Cursor].dbo.Tender t on rn.NotifNr=t.NotifNr join [Cursor].dbo.Lot l on t.Tender_ID=l.Tender_ID where TypeReasonFail_ID is null and Winner_ID is null and rc.RegSite_ID = " + reg_site_id0
        else:
            print(args)
            df_tenders_query1 = "SELECT rc.*, W.Referer, W.descr, W.Version FROM [CursorImport].[import].[RegionalCommon] rc left join [CursorImport].[import].[RegionalWebsites] W on rc.RegSite_ID=W.ID where Local_Notif_ID in " + str(args).replace('[','(').replace(']',')')
        #print(df_tenders_query1)
        df_tenders = parser_utils.select_query(df_tenders_query1, login_sql, password_sql, 'CursorImport')
        #print(df_tenders)
        #sys.exit()

        i = 0

        #df_sites_query = "select * FROM [CursorImport].[import].[RegionalWebsites] where id = " + reg_site_id0
        #df_sites = parser_utils.select_query(df_sites_query, login_sql, password_sql, 'CursorImport')

        if df_tenders.empty == True:
            isnotdone = False
            print('По этому региону нет новых протоколов')
        else:
            referer = df_tenders['Referer'][0]
            site = df_tenders['descr'][0]
            version = df_tenders['Version'][0]
            # site_id = df_tenders['id'][i]
            if args == None:
                print('Сейчас обрабатывается: ', site)

            for id0, row in df_tenders.iterrows():

                #Взять настройки сайта
                if args != None:
                    referer = row['Referer']
                    site = row['descr']
                    site_id = row['RegSite_ID']
                    version = row['Version']
                    print('Сейчас обрабатывается: ', site)

                #print('Сейчас обрабатывается: ', site)
                print(site, ':', id0, '/', len(df_tenders))

                for i in range(0, 10):
                    print('Попытка №' + str(i + 1))
                    try:
                        isError = False
                        # Vars
                        notif_id = row['id']
                        notif = row['Local_Notif_ID']

                        why_not = None
                        main_stat = None
                        prot_num = 0
                        Winner = None
                        Winner_ID = None
                        Winner_price = None
                        Winner_decision = None
                        Second = None
                        Second_ID = None
                        Second_price = None
                        Second_decision = None
                        Third = None
                        Third_ID = None
                        Third_price = None
                        Third_decision = None
                        Fourth = None
                        Fourth_ID = None
                        Fourth_price = None
                        Fourth_decision = None

                        url = referer + 'Trade/ViewTrade?id=' + str(notif)
                        headers = {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Content-type': 'application/json; charset=UTF-8',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                        }
                        print(url)
                        time.sleep(1)
                        if isProxy == True:
                            try:
                                r = requests.get(url, headers=headers, verify=False, proxies=proxies,
                                                 timeout=10)  # Пробуем подключиться
                            except requests.exceptions.Timeout as e:
                                print('Connection timeout')
                                isError = True
                        else:
                            r = requests.get(url, headers=headers, verify=False)
                        # print(r);time.sleep(60)
                        if isError == False:

                            #Получение статуса в зависимости от площадки
                            if version == 1:
                                try:
                                    #table1 = pd.read_html(r.text)[0]   #STEEL от 24.11.2020 Заголовок с данными в таблице 0
                                    table1 = pd.read_html(r.text)[1]  # Тендер
                                except:
                                    table1 = pd.read_html(r.text)[2]  # Тендер
                                # table3.to_excel('test_pos1.xlsx');print(table2);time.sleep(60)
                                # print(table1);print(len(table1))

                                tendStatus = None
                                if tendStatus == None:
                                    for st in range(len(table1)):
                                        if table1[0][st] == 'Статус':
                                            tendStatus = table1[1][st]

                                elif len(table1) == 12:
                                    uniqueId = None
                                    tendNm = table1[1][0]
                                    tendStatus = table1[1][1]
                                elif len(table1) == 13:
                                    if table1[0][0] != 'Наименование':
                                        uniqueId = table1[1][0]
                                        tendNm = table1[1][1]
                                        tendStatus = table1[1][2]
                                    else:
                                        uniqueId = None
                                        tendNm = table1[1][0]
                                        tendStatus = table1[1][1]
                                else:
                                    uniqueId = table1[1][0]
                                    tendNm = table1[1][1]
                                    tendStatus = table1[1][2]

                                #print(tendStatus)
                                #sys.exit()
                            if version == 2:
                                parsed_html1 = BeautifulSoup(r.text, features="html.parser")

                                tendStatus = \
                                    str(parsed_html1).split(
                                        '<p class="currentStatus active">')[1].split('</p>')[
                                        0].replace(
                                        '\r', '').replace('\n', '').replace('</span>', '').replace('<span>Статус: ', '')

                            #print(tendStatus)
                            #sys.exit()

                            if 'отменен' in tendStatus.lower():
                                prot_num = 0
                                why_not = "ЗАКУПКА ОТМЕНЕНА"
                                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Winner, Winner_ID,
                                                   Winner_price, Winner_decision, login_sql, password_sql, Debug=isDebug)
                                print(tendStatus)
                            elif 'не сост' in tendStatus.lower():
                                prot_num = 0
                                why_not = "По окончании срока подачи заявок не подано ни одной заявки"
                                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Winner, Winner_ID,
                                                   Winner_price, Winner_decision, login_sql, password_sql, Debug=isDebug)
                                print(tendStatus)
                            #STEEL от 08.04.2021 Если статус "Договор не заключён", то причина: Отказ от заключения контракта (для московской обл.)
                            elif 'оговор не заключ' in tendStatus.lower():
                                prot_num = 0
                                why_not = "Отказ от заключения контракта"
                                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Winner, Winner_ID,
                                                   Winner_price, Winner_decision, login_sql, password_sql, Debug=isDebug)
                                print(tendStatus)
                            else:
                                clear_winword_process()
                                try:
                                    os.remove('0.docx')
                                except:
                                    pass
                                try:
                                    os.remove('0.rtf')
                                except:
                                    pass

                                files = []
                                urls = []

                                #Достаём файлы
                                if version == 1:
                                    soup = BeautifulSoup(r.text, 'lxml')
                                    data_scripts = []
                                    data = soup.find_all('script', type='text/javascript')  # берем предпоследний скрипт
                                    for data0 in data:
                                        data1 = str(data0).replace('            ', '')
                                        data_split = data1.split('\n')  # Делим на строки
                                        data_scripts.append(data_split)

                                    for data2 in data_scripts:
                                        for spl in data2:  # Находим в строках соответствие
                                            if 'filename' in spl.lower():
                                                files.append(spl.lower())
                                            #STEEL от 05.03.2022 ссылка на файл начинается с 'url:'

                                            if 'url:' in spl.lower():
                                                if re.search('^url:', spl.lower()) != None:
                                                #if 'url:' in spl.lower() and ('files/FileDownloadHandler' in spl.lower() or 'api/Upload' in spl.lower()):
                                                #if ' url:' in spl.lower():
                                                    urls.append(spl.lower())

                                print(files)
                                print(urls)
                                #sys.exit()

                                Found = False
                                isrtf = False
                                for url, file in zip(urls, files):
                                    if 'протокол' in file.lower() or 'езультаты_закупки' in file.lower():  # Находим файлы протоколов
                                        url_download = url.split("url: '")[1].replace(' ', '').replace("'", "")
                                        r_download = requests.get(url_download, headers=headers, verify=False)
                                        with open('0.docx', 'wb') as f:  # Скачиваем документ
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


                                        # try:
                                        #     word = comtypes.client.CreateObject('Word.Application')
                                        #     doc = word.Documents.Open(path + '0.rtf')
                                        #     doc.SaveAs(path + '0.docx', FileFormat=wdFormatDOCX)
                                        #
                                        # except:
                                        #     word = win32com.client.DispatchEx("Word.Application")
                                        #     doc = word.Documents(path + '0.rtf')
                                        #     doc.SaveAs(path + '0.docx', FileFormat=wdFormatDOCX)


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
                                        #print(tables)
                                        #print(len(tables))
                                        #print(find_index_by_str(tables[3], 'Порядковый номер заявки', 1)[0] )
                                        winners_df = tables[0]  # Берем первую таблицу и редактируем
                                        winners_df.columns = winners_df.iloc[0]
                                        winners_df = winners_df.iloc[1:]
                                        winners_df = winners_df.reset_index(drop=True)
                                        #print(winners_df)
                                        #print(isrtf)
                                        if isrtf == True:  # Сохраняет docx он криво, поэтому дополнительно исправляем
                                            # winners_df = winners_df.drop([0])

                                            ind0 = find_index_by_str(winners_df, 'Порядковый номер заявки', 1)[
                                                0]  # Находим строку с порядковым номером и берем индекс
                                            winners_df.columns = winners_df.iloc[
                                                ind0]  # Подставляем строку с этим индексом в заголовок
                                            winners_df = winners_df.iloc[ind0 + 1:]  # Берем все строки ниже этой
                                            winners_df = winners_df.loc[:, ~winners_df.columns.duplicated()]
                                            winners_df = winners_df.loc[winners_df[winners_df.columns[1]].isin(
                                                ['1', '2', '3', '4', '5', '6', '7',
                                                 '8'])]  # Находим строки, где есть номера
                                            winners_df = winners_df[
                                                winners_df.columns[1:]]  # Убираем первую "пустую" колонну
                                            winners_df = winners_df.reset_index(drop=True)
                                            #print(winners_df);time.sleep(30)

                                        clear_winword_process()
                                        os.remove('0.docx')
                                        Found = True
                                    except:
                                        print('Не удалось открыть файл протоколов')
                                        traceback.print_exc()
                                        clear_winword_process()

                                if Found == False and 'заверш' not in tendStatus.lower():
                                    prot_num = 0
                                    why_not = "Документ протокола не найден. " + tendStatus
                                    main_stat = 'ПРОТОКОЛ НЕДОСТУПЕН'
                                    insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Winner, Winner_ID,
                                                       Winner_price, Winner_decision, login_sql, password_sql, Debug=isDebug)
                                    print('Документ протокола не найден')
                                    isnotdone = False
                                if Found == False and 'заверш' in tendStatus.lower():
                                    prot_num = 0
                                    why_not = "Закупка завершена, но документ отсутствует"
                                    main_stat = 'ПРОТОКОЛ НЕДОСТУПЕН'
                                    insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Winner, Winner_ID,
                                                       Winner_price, Winner_decision, login_sql, password_sql, Debug=isDebug)
                                    print('Документ протокола не найден')
                                    isnotdone = False

                                print(winners_df)
                                #sys.exit()


                                if winners_df is not None:
                                    for ind1, row in winners_df.iterrows():
                                        if 'не опред' in row[winners_df.columns[4]].lower() or 'побед' in row[
                                            winners_df.columns[4]].lower() or 'первый' in row[
                                            winners_df.columns[4]].lower() or 'первое' in row[
                                            winners_df.columns[4]].lower():
                                            if 'не опред' in row[winners_df.columns[4]].lower() and 'не соответ' in row[
                                                winners_df.columns[3]].lower():
                                                pass
                                            else:
                                                prot_num = 1
                                                main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                                Winner = row[winners_df.columns[1]]  # Наименование орга как в доке
                                                Winner_ID, inn, kpp = find_org_in_base(Winner, login_sql, password_sql)  # Находим орг в базе
                                                Winner_price = price_cleaner(row[winners_df.columns[2]])  # Очищаем цену
                                                Winner_decision = row[
                                                    winners_df.columns[3]]  # Проставляем вывод как в доке
                                                insert_prots_query(notif_id, notif, why_not, main_stat, prot_num,
                                                                   Winner, Winner_ID, Winner_price, Winner_decision, login_sql, password_sql,
                                                                   Debug=isDebug)
                                        elif 'втор' in row[winners_df.columns[4]].lower():
                                            prot_num = 2
                                            main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                            Second = row[winners_df.columns[1]]
                                            Second_ID, inn, kpp = find_org_in_base(Second, login_sql, password_sql)
                                            Second_price = price_cleaner(row[winners_df.columns[2]])
                                            Second_decision = row[winners_df.columns[3]]
                                            insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Second,
                                                               Second_ID, Second_price, Second_decision, login_sql, password_sql, Debug=isDebug)
                                        elif 'трет' in row[winners_df.columns[4]].lower() and 'место' in row[winners_df.columns[4]].lower():
                                            prot_num = 3
                                            main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                            Third = row[winners_df.columns[1]]
                                            Third_ID, inn, kpp = find_org_in_base(Third, login_sql, password_sql)
                                            Third_price = price_cleaner(row[winners_df.columns[2]])
                                            Third_decision = row[winners_df.columns[3]]
                                            insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Third,
                                                               Third_ID, Third_price, Third_decision, login_sql, password_sql, Debug=isDebug)
                                        elif 'четв' in row[winners_df.columns[4]].lower():
                                            prot_num = 4
                                            main_stat = 'ОПРЕДЕЛЕНИЕ ПОСТАВЩИКА ЗАВЕРШЕНО'
                                            Fourth = row[winners_df.columns[1]]
                                            Fourth_ID, inn, kpp = find_org_in_base(Fourth, login_sql, password_sql)
                                            Fourth_price = price_cleaner(row[winners_df.columns[2]])
                                            Fourth_decision = row[winners_df.columns[3]]
                                            insert_prots_query(notif_id, notif, why_not, main_stat, prot_num, Fourth,
                                                               Fourth_ID, Fourth_price, Fourth_decision, login_sql, password_sql, Debug=isDebug)


                            print('Done')
                            temp_done.append(notif)
                            break
                            # time.sleep(0.5)
                        else:
                            break
                    except:
                        traceback.print_exc()
                        time.sleep(15)
                        break

