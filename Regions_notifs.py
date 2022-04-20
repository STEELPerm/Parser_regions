import requests
import pandas as pd
import sys
import pyodbc
import time
from bs4 import BeautifulSoup
from datetime import datetime
from fuzzywuzzy import fuzz
import datetime as dt
import traceback
# lxml

import parser_utils
import Org_creator

def load_notifs_to_import(login_sql, password_sql, t2, isArgs=False, args_to_parse=None, isDebug=True, isProxy=True):
    # Достаем прокси
    if isProxy == True:
        df_proxies = parser_utils.get_proxies(login_sql, password_sql)

    # Проставить NotNeed = 1 у ненужных извещений (проверяется TendNm по чёрному списку слов)
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=37.203.243.65\CURSORMAIN,49174;'
                          'Database=CursorImport;'
                          'UID=' + login_sql + ';'
                          'PWD=' + password_sql + ';'
                          'Trusted_Connection=no;')
    cursor = conn.cursor()
    cursor.execute("exec spRegion_RegionalCommon_setNotNeed")

    if isDebug == False:
        conn.commit()
    conn.close()


    # Достаем инфу о сайтах
    reg_sites_query = "SELECT id, isnull(Version,1) as version FROM [CursorImport].[import].[RegionalWebsites]" #where Host = 'zmo-new-webapi.rts-tender.ru'
    df_reg_site = parser_utils.select_query(reg_sites_query, login_sql, password_sql)

    # Забираем статусы из базы
    status_query = "SELECT * FROM [Cursor].[dbo].[StatusT] where id != 0"
    df_status = parser_utils.select_query(status_query, login_sql, password_sql)

    # Забираем единицы измерения из базы
    ed_izm_query = "SELECT Units_ID, UnitsName, ShortName from [Cursor].[dbo].[Units] where Units_ID != 0"
    df_ed_izm = parser_utils.select_query(ed_izm_query, login_sql, password_sql)

    # Забираем регионы из базы для поиска
    reg_query = "SELECT * FROM [Cursor].[dbo].[Region] where RegNm != '~'"
    dr_regs = parser_utils.select_query(reg_query, login_sql, password_sql)

    for df_id, reg_site_id in df_reg_site.iterrows():
        reg_site_id0 = str(reg_site_id['id'])
        version = reg_site_id['version']
        isnotdone = True
        loaded = []
        for i in range(0, 10):
            while isnotdone == True:
                # try:
                print('Попытка №' + str(i + 1))
                if isProxy == True:
                    proxy = df_proxies.sample(1)['proxy'].reset_index(drop=True)[
                        0]  # Берем один рандомный прокси из DataFrame
                    print('Подключаюсь, используя прокси: ' + str(proxy))
                    prox = str(proxy).replace(' ', '')
                    proxies = {'http': 'http://' + prox, 'https': 'http://' + prox, }
                try:
                    if isArgs == False:
                        #df_tenders_query1 = "SELECT rc.* FROM [CursorImport].[import].[RegionalCommon] rc left join [CursorImport].[import].Regional_Notif rn on rc.Local_Notif_ID=rn.NotifNr where rn.NotifNr is null and rc.PublDt >= '" + t2 + "' and isnull(rc.NotNeed,0)=0 and rc.RegSite_ID = " + reg_site_id0
                        #за 2022 год
                        df_tenders_query1 = "SELECT rc.* FROM [CursorImport].[import].[RegionalCommon] rc left join [CursorImport].[import].Regional_Notif rn on rc.Local_Notif_ID=rn.NotifNr where rn.NotifNr is null and rc.PublDt >= '20220101' and isnull(rc.NotNeed,0)=0 and rc.RegSite_ID = " + reg_site_id0
                    else:
                        args = str(args_to_parse).replace('[', '').replace(']', '')
                        df_tenders_query1 = "SELECT * FROM [CursorImport].[import].[RegionalCommon]  where Local_Notif_ID in ("+args+") and RegSite_ID = " + reg_site_id0
                    df_tenders = parser_utils.select_query(df_tenders_query1, login_sql, password_sql, 'CursorImport')

                    df_sites_query = "select * FROM [CursorImport].[import].[RegionalWebsites] where id = " + reg_site_id0
                    df_sites = parser_utils.select_query(df_sites_query, login_sql, password_sql, 'CursorImport')

                    i = 0
                    referer = df_sites['Referer'][i]
                    site = df_sites['descr'][i]
                    site_id = df_sites['id'][i]
                    print('Сейчас обрабатывается: ', site)
                    if df_tenders.empty == True:
                        isnotdone = False
                        print('По этому региону нет новых извещений')
                    else:
                        for id0, row in df_tenders.iterrows():
                            isError = False
                            notif = row['Local_Notif_ID']

                            if notif not in loaded:
                                # check doubles!
                                df_tender_double_query = 'SELECT Notif_ID FROM [CursorImport].[import].Regional_Notif ' \
                                                         "where NotifNr = '" + str(notif) + "' "
                                df_tender_double = parser_utils.select_query(df_tender_double_query, login_sql, password_sql, 'CursorImport')

                                if df_tender_double.empty == True or isArgs == True:
                                    url = referer + 'Trade/ViewTrade?id=' + str(notif)
                                    headers = {
                                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                        'Content-type': 'application/json; charset=UTF-8',
                                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                                    }
                                    print(url)
                                    time.sleep(1)
                                    if isDebug == False and isProxy == True:
                                        try:
                                            r = requests.get(url, headers=headers, verify=False, proxies=proxies,
                                                             timeout=10)  # load_notifs_to_import
                                            # робуем подключиться
                                        except requests.exceptions.Timeout as e:
                                            print('Connection timeout')
                                            isError = True
                                    else:
                                        try:
                                            r = requests.get(url, headers=headers, verify=False, timeout=120)
                                        except requests.exceptions.Timeout as e:
                                            print('Connection timeout')
                                            isError = True
                                    if isError == False:
                                        #print(version)

                                        if version == 1:
                                            try:
                                                table1 = pd.read_html(r.text.replace(',00', ''))[1]  # Тендер
                                                #table1 = pd.read_html(r.text)[1]  # Тендер
                                                table2 = pd.read_html(r.text)[2]  # Наименования колонок позиций
                                                table3 = pd.read_html(str(r.text).replace(',', '.'))[3]  # Сами позиции
                                                table3.columns = table2.columns
                                                try:
                                                    table4 = pd.read_html(r.text)[4]
                                                except:
                                                    table4 = None
                                            except:
                                                table1 = pd.read_html(r.text)[2]  # Тендер
                                                table2 = pd.read_html(r.text)[3]  # Наименования колонок позиций
                                                # table3 = pd.read_html(r.text)[4]  # Сами позиции
                                                table3 = pd.read_html(str(r.text).replace(',', '.'))[4]  # Сами позиции
                                                table3.columns = table2.columns
                                            # print(str(r.text).replace(',', '.'))
                                            # table3.to_excel('test_pos1.xlsx')
                                            # print(table3);time.sleep(60)
                                            #print(r.text.re.sub('\d[,]\d', '\d[.]\d', r.text, count=0))
                                            #print(table1);print(len(table1))
                                            # sys.exit()
                                            RNPlanGraphNum = None

                                            #если есть раздел документы закупки номер должен уйти на ручной ввод, если имеем только объект закупки номер вводим автоматом
                                            if table4 is not None:
                                                havedocs = 1
                                            else:
                                                havedocs = 0

                                            contractDt = None
                                            if len(table1) == 12:
                                                if table1[0][0] == 'Идентификационный код закупки':
                                                    uniqueId = table1[1][0]
                                                    tendNm = table1[1][1]
                                                    tendStatus = table1[1][2]
                                                    if table1[0][3] == 'Причина отмены':
                                                        price = str(table1[1][4]).replace(' ', '').replace(',', '.')
                                                        fz = table1[1][5]
                                                    else:
                                                        price = str(table1[1][3]).replace(' ', '').replace(',', '.')
                                                        fz = table1[1][4]
                                                    deliverDt = table1[1][6]
                                                    deliverPlace = table1[1][7]
                                                    payment = table1[1][8]
                                                    descr = table1[1][9]
                                                    tendEnd = table1[1][10].split(' ')[0]
                                                    contractDt = table1[1][11]
                                                else:
                                                    uniqueId = None
                                                    tendNm = table1[1][0]
                                                    tendStatus = table1[1][1]
                                                    price = str(table1[1][2]).replace(' ', '').replace(',', '.')
                                                    fz = table1[1][3]
                                                    deliverDt = table1[1][6]
                                                    deliverPlace = table1[1][7]
                                                    payment = table1[1][8]
                                                    descr = table1[1][9]
                                                    tendEnd = table1[1][10].split(' ')[0]
                                                    contractDt = table1[1][11]
                                            elif len(table1) == 13:
                                                if table1[0][0] != 'Наименование':
                                                    uniqueId = table1[1][0]
                                                    tendNm = table1[1][1]
                                                    tendStatus = table1[1][2]
                                                    price = str(table1[1][3]).replace(' ', '').replace(',', '.')
                                                    fz = table1[1][4]
                                                    deliverDt = table1[1][7]
                                                    deliverPlace = table1[1][8]
                                                    payment = table1[1][9]
                                                    descr = table1[1][10]
                                                    tendEnd = table1[1][11].split(' ')[0]
                                                    contractDt = table1[1][12]
                                                else:
                                                    uniqueId = None
                                                    tendNm = table1[1][0]
                                                    tendStatus = table1[1][1]
                                                    price = str(table1[1][3]).replace(' ', '').replace(',', '.')
                                                    fz = table1[1][4]
                                                    deliverDt = table1[1][7]
                                                    deliverPlace = table1[1][8]
                                                    payment = table1[1][9]
                                                    descr = table1[1][10]
                                                    tendEnd = table1[1][11].split(' ')[0]
                                                    contractDt = table1[1][12]
                                            elif len(table1) == 14:
                                                uniqueId = table1[1][0]
                                                tendNm = table1[1][1]
                                                tendStatus = table1[1][2]
                                                cause = table1[1][3]
                                                price = str(table1[1][4]).replace(' ', '').replace(',', '.')
                                                fz = table1[1][5]
                                                deliverDt = table1[1][8]
                                                deliverPlace = table1[1][9]
                                                payment = table1[1][10]
                                                descr = table1[1][11]
                                                tendEnd = table1[1][12].split(' ')[0]
                                                contractDt = table1[1][13]
                                            elif len(table1) == 11 and table1[0][1] == 'Наименование':
                                                uniqueId = table1[1][0]
                                                tendNm = table1[1][1]
                                                tendStatus = table1[1][2]
                                                price = str(table1[1][3]).replace(' ', '').replace(',', '.')
                                                fz = table1[1][4]
                                                deliverDt = table1[1][5]
                                                deliverPlace = table1[1][6]
                                                payment = table1[1][7]
                                                descr = table1[1][8]
                                                tendEnd = table1[1][9].split(' ')[0].replace(' (по московскому времени)', '')
                                                contractDt = table1[1][10]
                                            elif len(table1) == 11 and table1[0][0] == 'Наименование':
                                                uniqueId = None
                                                tendNm = table1[1][0]
                                                tendStatus = table1[1][1]
                                                price = str(table1[1][3]).replace(' ', '').replace(',', '.')
                                                fz = table1[1][4]
                                                deliverDt = table1[1][5]
                                                deliverPlace = table1[1][6]
                                                payment = table1[1][7]
                                                descr = table1[1][8]
                                                tendEnd = table1[1][9].split(' ')[0].replace(' (по московскому времени)', '')
                                                contractDt = table1[1][10]
                                            else:
                                                uniqueId = None
                                                tendNm = table1[1][0]
                                                tendStatus = table1[1][1]
                                                price = str(table1[1][2]).replace(' ', '').replace(',', '.')
                                                fz = table1[1][3]
                                                deliverDt = table1[1][4]
                                                deliverPlace = table1[1][5]
                                                payment = table1[1][6]
                                                descr = table1[1][7]
                                                tendEnd = table1[1][8].split(' ')[0]
                                                contractDt = table1[1][9]

                                                #uniqueId = table1[1][0]
                                                #tendNm = table1[1][1]
                                                #tendStatus = table1[1][2]
                                                #price = str(table1[1][3]).replace(' ', '').replace(',', '.')
                                                #fz = table1[1][4]
                                                #deliverDt = table1[1][6]
                                                #deliverPlace = table1[1][7]
                                                #payment = table1[1][8]
                                                #descr = table1[1][9]
                                                #tendEnd = table1[1][10].split(' ')[0]
                                                #contractDt = table1[1][11]

                                        elif version == 2:
                                            parsed_html1 = BeautifulSoup(r.text, features="html.parser")
                                            RNPlanGraphNum = None
                                            fz = None
                                            deliverPlace = None
                                            tendEnd = None
                                            contractDt = None

                                            tendStatus = \
                                                str(parsed_html1).split(
                                                    '<p class="currentStatus active">')[1].split('</p>')[
                                                    0].replace(
                                                    '\r', '').replace('\n', '').replace('</span>', '').replace('<span>Статус: ', '')
                                            #print(tendStatus)

                                            items = parsed_html1.find_all('div',class_='informationAboutCustomer__informationPurchase-infoBlock infoBlock')

                                            for item in items:
                                                #print(item)
                                                item2 = item.find('span', class_='infoBlock__label').get_text(strip=True)

                                                if item2 == 'Наименование:':
                                                    tendNm = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()
                                                if item2 == 'МЦК, ₽:':
                                                    price = item.find('p', class_='infoBlock__text price').get_text(strip=True).lstrip().rstrip()
                                                if item2 == 'Платформа источник:':
                                                    fz = item.find('p', class_='infoBlock__text price').get_text(strip=True).lstrip().rstrip()
                                                if item2 == 'Сроки поставки:':
                                                    deliverDt = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()
                                                if item2 == 'Место поставки:':
                                                    deliverPlace = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()
                                                if item2 == 'р/н лота позиции плана-графика ЕАСУЗ:':
                                                    RNPlanGraphNum = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()

                                                #if item2 == 'Условия оплаты:':
                                                #    payment = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()
                                                #if item2 == 'Описание:':
                                                #    descr = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()

                                                if item2 == 'Дата окончания подачи предложений:':
                                                    tendEnd = item.find('p', class_='infoBlock__text localize_datetime_seconds').get_text(strip=True).lstrip().rstrip()
                                                if item2 == 'Плановая дата заключения договора:':
                                                    contractDt = item.find('p', class_='infoBlock__text').get_text(strip=True).lstrip().rstrip()

                                        uniqueId = None
                                        #print(tendNm)

                                        cust = row['Customer_ID']
                                        org_ID_cust = row['Org_ID']
                                        cust_INN = row['Customer_INN']
                                        publDt = str(row['PublDt']).replace("'", '')   #;print(publDt)

                                        if version == 2:
                                            try:
                                                publDt = datetime.strptime(publDt, '%Y-%m-%d %H:%M:%S.%f')
                                            except:
                                                publDt = datetime.strptime(publDt, '%Y-%m-%d %H:%M:%S')
                                        else:
                                            publDt = datetime.strptime(publDt, '%Y-%m-%d %H:%M:%S')

                                        publDt = publDt.strftime("%Y%m%d")
                                        publDt = "'" + publDt + "'"


                                        if 'nan' in str(org_ID_cust).lower():
                                            org_ID_cust = None

                                        if 'nan' in str(contractDt).lower():
                                            contractDt = None
                                        # time.sleep(60)

                                        if 'nan' in str(deliverDt).lower():
                                            deliverDt = None

                                        if version == 1:
                                            if str(payment).lower() == 'nan':
                                                payment = None
                                            if str(descr).lower() == 'nan':
                                                descr = tendNm
                                        else:
                                            payment = None
                                            descr = tendNm

                                        if '44' in fz:
                                            fz_id = 44
                                        elif '223' in fz:
                                            fz_id = 223
                                        else:
                                            fz_id = 3

                                        # Находим регион
                                        reg_ids = []
                                        for reg_id, reg in dr_regs.iterrows():
                                            reg0 = reg['RegNm']
                                            compare_ratio0 = fuzz.token_set_ratio(reg0, deliverPlace)
                                            # print(compare_ratio0, reg, deliverPlace)
                                            if compare_ratio0 > 90:
                                                reg_ids.append(reg['Reg_ID'])
                                        try:
                                            reg_id_fin = reg_ids[0]
                                        except:
                                            reg_id_fin = None
                                        if reg_id_fin == None:
                                            reg_ids = []
                                            for reg_id, reg in dr_regs.iterrows():
                                                reg0 = reg['RegNm']
                                                compare_ratio0 = fuzz.token_set_ratio(reg0, site)
                                                # print(compare_ratio0, reg, deliverPlace)
                                                if compare_ratio0 > 90:
                                                    reg_ids.append(reg['Reg_ID'])
                                            try:
                                                reg_id_fin = reg_ids[0]
                                            except:
                                                reg_id_fin = None

                                        #print(price);time.sleep(60)

                                        if ',' or ' ' or ' ' in price:
                                          price = price.replace(",", ".").replace(" ", "").replace(" ", "")
                                        if parser_utils.is_number(price) == False:
                                            #STEEL от 04.04.2022 Сделал =0, т.к. при вставке в базу: Conversion failed when converting the nvarchar value 'None' to data type int.
                                            #price = None
                                            price = 0

                                        # Определяем статус по базе
                                        status_list = []
                                        for stat_id, status0 in df_status.iterrows():
                                            status1 = status0['name']
                                            compare_ratio1 = fuzz.token_set_ratio(status1, tendStatus)

                                            if compare_ratio1 > 60:
                                                status_list.append(status0['id'])
                                        try:
                                            status_fin = status_list[0]
                                        except:
                                            if tendStatus.lower() == 'завершен' \
                                                    or tendStatus.lower() == 'завершена' \
                                                    or tendStatus.lower() == 'договор заключен' \
                                                    or tendStatus.lower() == 'заключение договора' \
                                                    or tendStatus.lower() == 'согласование':
                                                status_fin = 4
                                            elif tendStatus.lower() == 'прием предложений' or tendStatus.lower() == 'рассмотрение предложений':
                                                status_fin = 2
                                            elif tendStatus.lower() == 'решение заказчика о внесении изменений в закупку' or tendStatus.lower() == 'заключение контрактов': # or tendStatus.lower() == 'согласование'
                                                status_fin = 7
                                            elif tendStatus.lower() == 'техническая ошибка':
                                                status_fin = 5
                                            elif tendStatus.lower() == 'отменен' or tendStatus.lower() == 'отменена':
                                                status_fin = 5
                                            else:
                                                status_fin = None

                                        # Забираем заказчика из базы
                                        #print(org_ID_cust, cust)
                                        #sys.exit()
                                        org_cust = None

                                        if org_ID_cust == None:
                                            if cust != None:
                                                org_cust_query = "SELECT o.Org_ID from [Cursor].[dbo].[Org] o join [CursorImport].[import].[RegionalOrgs] ro on o.INN=ro.Customer_INN and o.KPP=ro.Customer_KPP where ro.Customer_ID = " + str(
                                                cust)
                                                org_cust = parser_utils.select_query(org_cust_query, login_sql, password_sql, 'Cursor')

                                                org_cust = org_cust['Org_ID'][0]
                                            if cust != None and org_cust.empty == True:  # если не находим, згначит пытаемся загрузить новый орг в базу
                                                org_inn_query = "SELECT [Customer_INN] from [CursorImport].[import].[RegionalOrgs] where Customer_ID = " + str(cust)
                                                org_inn = parser_utils.select_query(org_inn_query, login_sql, password_sql, 'Cursor')['Customer_INN'][0].replace(' ', '')
                                                resp1 = Org_creator.create(org_inn, login_sql, password_sql)  # добавляем в базу с помощью функции
                                                print(resp1)
                                                org_cust_query = "SELECT o.Org_ID from [Cursor].[dbo].[Org] o join [CursorImport].[import].[RegionalOrgs] ro on o.INN=ro.Customer_INN and o.KPP=ro.Customer_KPP where ro.Customer_ID = " + str(
                                                    cust)
                                                org_cust = parser_utils.select_query(org_cust_query, login_sql, password_sql, 'Cursor')
                                                #org_cust = org_cust['Org_ID'][0]
                                                org_cust = org_cust['Org_ID'][0]

                                            #print(org_cust, cust, cust_INN)
                                            #sys.exit()

                                            if cust == None and cust_INN != None:
                                                resp1 = Org_creator.create(cust_INN, login_sql,password_sql)  # добавляем в базу с помощью функции
                                                print(resp1)
                                                conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                      'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                      'Database=CursorImport;'
                                                                      'UID=' + login_sql + ';'
                                                                      'PWD=' + password_sql + ';'
                                                                      'Trusted_Connection=no;')
                                                cursor = conn.cursor()
                                                cursor.execute(
                                                    "exec [CursorImport].[import].[spRegions_FillOrg] " + reg_site_id0)
                                                conn.commit()
                                                conn.close()

                                                org_cust_query = "SELECT Org_ID from [CursorImport].[import].[RegionalCommon] where Local_Notif_ID in ("+str(notif)+")"
                                                org_cust = parser_utils.select_query(org_cust_query, login_sql,password_sql, 'CursorImport')
                                                org_cust = org_cust['Org_ID'][0]

                                        if org_ID_cust != None:
                                            org_cust = org_ID_cust

                                        if tendNm == None and descr != None:
                                            tendNm = descr

                                        try:
                                            org_cust = int(org_cust)
                                        except:
                                            pass

                                        # print(price, str(tendNm), str(deliverPlace), reg_id_fin, str(tendStatus),status_fin, deliverDt)
                                        # print(publDt, tendEnd, contractDt, url, str(notif), cust, str(org_cust),str(deliverDt), str(tendNm),
                                        #      price, payment, descr, str(site_id),uniqueId, fz_id)
                                        #sys.exit()


                                        if isArgs == True:  # Если перезаливка - удаляем извещения с импорта
                                            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                  'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                  'Database=CursorImport;'
                                                                  'UID=' + login_sql + ';'
                                                                                       'PWD=' + password_sql + ';'
                                                                                                               'Trusted_Connection=no;')
                                            cursor = conn.cursor()
                                            cursor.execute("delete ls " \
                                                           "FROM [CursorImport].[import].[Regional_Notif] n (nolock) " \
                                                           "inner join [CursorImport].[import].Regional_LotSpec ls (nolock) on n.Notif_ID=ls.Notif_ID " \
                                                           "where n.NotifNr = '" + str(notif) + "'")
                                            cursor.execute("delete l " \
                                                           "FROM [CursorImport].[import].[Regional_Notif] n (nolock) " \
                                                           "inner join [CursorImport].[import].Regional_Lot l (nolock) on n.Notif_ID=l.Notif_ID " \
                                                           "where n.NotifNr = '" + str(notif) + "'")
                                            cursor.execute("delete n " \
                                                           "FROM [CursorImport].[import].[Regional_Notif] n (nolock) " \
                                                           "where n.NotifNr = '" + str(notif) + "'")
                                            if isDebug == False:
                                                conn.commit()
                                            conn.close()
                                        print('Закачиваем извещение: ', notif)


                                        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                              'Server=37.203.243.65\CURSORMAIN,49174;'
                                                              'Database=CursorImport;'
                                                              'UID='+login_sql+';'
                                                              'PWD='+password_sql+';'
                                                              'Trusted_Connection=no;')
                                        cursor = conn.cursor()
                                        # Noifs
                                        if contractDt != None:
                                            cursor.execute(
                                                "insert into [CursorImport].[import].[Regional_Notif]([PublDt], [Url], [NotifNr], [Customer_ID], [Org_ID], [ClaimReglament], [TendDt], [TendNm], [TenderPrice], [Condition], [Descr], [TendEndDt], [ContractPlannedDt], [RegSiteID], [UniqueIdentificator], [SYSDATE], FZ_ID_import)"
                                                "values (CAST(" + publDt + " as DATE), ?, ?, ?, ?, ?, CAST(" + publDt + " as DATE), ?, ?, ?, ?, convert(date,'" + tendEnd + "',104), convert(date,'" + contractDt + "',104), ?, ?, getdate(), ?) ",
                                                url, str(notif), cust, str(org_cust), str(deliverDt), str(tendNm), price, #convert(decimal(22,5),"+str(price)+")
                                                payment,descr, #str(tendEnd), contractDt,
                                                str(site_id), uniqueId, fz_id
                                            )
                                        else:
                                            cursor.execute(
                                                "insert into [CursorImport].[import].[Regional_Notif]([PublDt], [Url], [NotifNr], [Customer_ID], [Org_ID], [ClaimReglament], [TendDt], [TendNm], [TenderPrice], [Condition], [Descr], [TendEndDt], [RegSiteID], [UniqueIdentificator], [SYSDATE], FZ_ID_import)"
                                                "values (CAST(" + publDt + " as DATE), ?, ?, ?, ?, ?, CAST(" + publDt + " as DATE), ?, ?, ?, ?, convert(date,'" + tendEnd + "',104), ?, ?, getdate(), ?) ",
                                                url, str(notif), cust, str(org_cust), str(deliverDt), str(tendNm),
                                                price, #convert(decimal(22,5),"+str(price)+")
                                                payment, descr,  # str(tendEnd), contractDt,
                                                str(site_id), uniqueId, fz_id
                                            )

                                        #print(str(tendNm), price,str(deliverPlace),reg_id_fin,str(tendStatus),status_fin,deliverDt, RNPlanGraphNum)
                                        #sys.exit()

                                        # Lots
                                        cursor.execute(
                                            "declare @i_notif int; Select @i_notif = Notif_ID FROM [CursorImport].[import].[Regional_Notif] where NotifNr = '" + str(
                                                notif) + "'; "
                                                         "insert into [CursorImport].[import].[Regional_Lot]([Notif_ID], [LotNm], [PriceStart], [DeliveryPlace], [Reg_ID], [LotStatus], [LotStatus_ID], [DeliveryDtSite], [SYSDATE], RNPlanGraphNum)"
                                                         "values (@i_notif, ?, ?, ?, ?, ?, ?, ?, getdate(), ?) ",
                                            str(tendNm), price, #convert(decimal(22,5),"+str(price)+")
                                            str(deliverPlace), reg_id_fin, str(tendStatus), status_fin,
                                            deliverDt, RNPlanGraphNum #str(deliverDt)
                                        )
                                        if isDebug == False:
                                            conn.commit()
                                        conn.close()

                                        # LotSpecs
                                        if version == 1:
                                            for spec_id, spec in table3.iterrows():
                                                prodNm = str(spec['Наименование товара, работ, услуг'])
                                                okpd_code = spec['Код классификатора'].split(' / ')[0]
                                                ed_izm = spec['Единицы измерения']
                                                try:
                                                    okpd_name = spec['Код классификатора'].split(' / ')[1]
                                                except:
                                                    okpd_name = None

                                                # Количество
                                                try:
                                                    qty = int(spec['Количество'])
                                                except:
                                                    #qty = None
                                                    qty = 0

                                                # Тип продукта
                                                prodtype = '-'
                                                okpd_code = okpd_code.replace('/', '').replace(' ', '')
                                                price_spec = None

                                                if len(table3) == 1:
                                                    summ = price
                                                    if qty != None and summ != None:
                                                        try:
                                                            price_spec = float(summ) / qty
                                                        except:
                                                            price_spec = None
                                                else:
                                                    # Стоимость
                                                    try:
                                                        price_spec = float(str(spec['Стоимость единицы продукции']).replace(' ', '').replace(',', '.'))
                                                        # price_spec = str(spec['Стоимость единицы продукции']).replace(' ', '').split(',')[0]
                                                    except:
                                                        price_spec = None

                                                    # Сумма
                                                    try:
                                                        summ = float(str(spec['Стоимость поставляемого товара, выполняемых работ, оказываемых услуг']).replace(' ', ''))
                                                    except:
                                                        summ = None

                                                    try:
                                                        if price_spec != None:
                                                            summ = float(price_spec) * qty
                                                        else:
                                                            summ = None
                                                    except:
                                                        pass

                                                # print(okpd_name ,price_spec, qty, summ); time.sleep(60)
                                                # Единица измерения
                                                for index_ei, row_ei in df_ed_izm.iterrows():
                                                    ed_izm_id = row_ei['Units_ID']
                                                    full_ei = row_ei['UnitsName']
                                                    short_ei = row_ei['ShortName']
                                                    compare_ratio_ei1 = fuzz.token_set_ratio(full_ei, ed_izm)
                                                    compare_ratio_ei2 = fuzz.token_set_ratio(short_ei, ed_izm)
                                                    if 'штук' in str(ed_izm).lower():
                                                        ed_izm_base = 2
                                                    elif 'миллиграм' in str(ed_izm).lower():
                                                        ed_izm_base = 31
                                                    elif 'миллилитр' in str(ed_izm).lower():
                                                        ed_izm_base = 11
                                                    elif 'комплект' in str(ed_izm).lower():
                                                        ed_izm_base = 29
                                                    elif 'упаков' in str(ed_izm).lower():
                                                        ed_izm_base = 1
                                                    elif 'флакон' in str(ed_izm).lower():
                                                        ed_izm_base = 3
                                                    elif 'шт.' in str(ed_izm).lower():
                                                        ed_izm_base = 2
                                                    elif 'доза' in str(ed_izm).lower():
                                                        ed_izm_base = 9
                                                    elif 'таблет' in str(ed_izm).lower():
                                                        ed_izm_base = 4
                                                    elif 'блистер' in str(ed_izm).lower():
                                                        ed_izm_base = 6
                                                    elif 'капсул' in str(ed_izm).lower():
                                                        ed_izm_base = 7
                                                    elif 'грамм' in str(ed_izm).lower():
                                                        ed_izm_base = 20
                                                    elif compare_ratio_ei1 > 75:
                                                        ed_izm_base = ed_izm_id
                                                    elif compare_ratio_ei2 > 75:
                                                        ed_izm_base = ed_izm_id
                                                    else:
                                                        ed_izm_base = 46

                                                # Форма
                                                form = prodNm
                                                if prodNm != None:
                                                    prodNm = prodNm[:499]
                                                print(prodtype, str(prodNm), form, okpd_code, okpd_name, price_spec, str(qty), summ, str(ed_izm), ed_izm_base)
                                                conn1 = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                       'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                       'Database=CursorImport;'
                                                                       'UID='+login_sql+';'
                                                                       'PWD='+password_sql+';'
                                                                       'Trusted_Connection=no;')
                                                cursor1 = conn1.cursor()

                                                cursor1.execute(
                                                    "declare @i_notif int; Select @i_notif = Notif_ID FROM [CursorImport].[import].[Regional_Notif] where NotifNr = '" + str(
                                                        notif) + "'; "
                                                                 "insert into [CursorImport].[import].[Regional_LotSpec]([Notif_ID], [ProdType], [ProdNm], [Form], [OKPD_code], [OKPD_name], [Price], [Amount], [Summ], [SYSDATE], Measure, Measure_base, havedocs)"
                                                                 "values (@i_notif, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, ?) ",
                                                    prodtype, str(prodNm), form, okpd_code, okpd_name, price_spec, str(qty), summ,
                                                    str(ed_izm), ed_izm_base, havedocs
                                                )
                                                if isDebug == False:
                                                    conn1.commit()
                                                conn1.close()
                                        if version == 2:
                                            items = parsed_html1.find_all('div',class_='outputResults__oneResult')

                                            #Перебор элементов
                                            for item in items:
                                                # Левый столбец
                                                items2 = item.find_all('p', class_='leftPart__parag')
                                                for item2 in items2:
                                                    item3 = item2.find('span', class_='grayText').get_text(strip=True)

                                                    if item3 == 'Наименование товара, работ, услуг:':
                                                        prodNm = str(item2).split(
                                                            '<span class="grayText">Наименование товара, работ, услуг:</span>')[
                                                            1].split('</p>')[0].replace('\r', '').replace('\n','').replace(
                                                            '</p>', '').replace('<p>', '').lstrip().rstrip()
                                                    if item3 == 'Код классификатор:':
                                                        try:
                                                            okpd_code = item2.find('a').get_text(strip=True).lstrip().rstrip()
                                                        except:
                                                            okpd_code = str(item2).split(
                                                            '<span class="grayText">Код классификатор:</span>')[
                                                            1].split('</p>')[
                                                            0].replace('\r', '').replace('\n','').replace(
                                                            '</p>', '').replace('<p>', '').replace('<span>','').replace('</span>', '').lstrip().rstrip()
                                                    if item3 == 'Тип классификатор:':
                                                        okpd_type = str(item2).split(
                                                            '<span class="grayText">Тип классификатор:</span>')[
                                                            1].split('</p>')[0].replace('\r', '').replace('\n','').replace(
                                                            '</p>', '').replace('<p>', '').replace('<span>', '').replace('</span>', '').lstrip().rstrip()

                                                #Центральный столбец
                                                items2 = item.find_all('p', class_='centerPart__contentResult-parag')
                                                for item2 in items2:
                                                    item3 = item2.find('span', class_='grayText').get_text(strip=True)

                                                    if item3 == 'Единицы измерения:':
                                                        ed_izm = str(item2).split(
                                                            '<span class="grayText">Единицы измерения:</span>')[
                                                            1].split('</p>')[0].replace('\r', '').replace('\n',
                                                                                                          '').replace(
                                                            '</p>', '').replace('<p>', '').lstrip().rstrip()
                                                    if item3 == 'Количество:':
                                                        qty = str(item2).split(
                                                            '<span class="grayText">Количество:</span>')[
                                                            1].split('</p>')[0].replace('\r', '').replace('\n',
                                                                                                          '').replace(
                                                            '</p>', '').replace('<p>', '').lstrip().rstrip()

                                                #Правый столбец
                                                items2 = item.find_all('p', class_='rightPart__contentResult-parag')
                                                for item2 in items2:
                                                    item3 = item2.find('span', class_='grayText').get_text(strip=True)

                                                    if item3 == 'Стоимость единицы продукции ( в т.ч. НДС при наличии):':
                                                        price_spec = str(item2).split(
                                                            '<span class="grayText">Стоимость единицы продукции ( в т.ч. НДС при наличии):</span>')[
                                                            1].split('</p>')[0].replace('\r', '').replace('\n',
                                                                                                          '').replace(
                                                            '</p>', '').replace('<p>', '').lstrip().rstrip()
                                                    if item3 == 'Стоимость поставленого товара, выполненых работ, оказываемых услуг ( в т.ч. НДС при наличии):':
                                                        summ = str(item2).split(
                                                            '<span class="grayText">Стоимость поставленого товара, выполненых работ, оказываемых услуг ( в т.ч. НДС при наличии):</span>')[
                                                            1].split('</p>')[0].replace('\r', '').replace('\n',
                                                                                                          '').replace(
                                                            '</p>', '').replace('<p>', '').lstrip().rstrip()

                                                okpd_name = None

                                                # Тип продукта
                                                prodtype = '-'
                                                okpd_code = okpd_code.replace('/', '').replace(' ', '')

                                                # print(okpd_name ,price_spec, qty, summ); time.sleep(60)
                                                # Единица измерения
                                                for index_ei, row_ei in df_ed_izm.iterrows():
                                                    ed_izm_id = row_ei['Units_ID']
                                                    full_ei = row_ei['UnitsName']
                                                    short_ei = row_ei['ShortName']
                                                    compare_ratio_ei1 = fuzz.token_set_ratio(full_ei, ed_izm)
                                                    compare_ratio_ei2 = fuzz.token_set_ratio(short_ei, ed_izm)
                                                    if 'штук' in str(ed_izm).lower():
                                                        ed_izm_base = 2
                                                    elif 'миллиграм' in str(ed_izm).lower():
                                                        ed_izm_base = 31
                                                    elif 'миллилитр' in str(ed_izm).lower():
                                                        ed_izm_base = 11
                                                    elif 'комплект' in str(ed_izm).lower():
                                                        ed_izm_base = 29
                                                    elif 'упаков' in str(ed_izm).lower():
                                                        ed_izm_base = 1
                                                    elif 'флакон' in str(ed_izm).lower():
                                                        ed_izm_base = 3
                                                    elif 'шт.' in str(ed_izm).lower():
                                                        ed_izm_base = 2
                                                    elif 'доза' in str(ed_izm).lower():
                                                        ed_izm_base = 9
                                                    elif 'таблет' in str(ed_izm).lower():
                                                        ed_izm_base = 4
                                                    elif 'блистер' in str(ed_izm).lower():
                                                        ed_izm_base = 6
                                                    elif 'капсул' in str(ed_izm).lower():
                                                        ed_izm_base = 7
                                                    elif 'грамм' in str(ed_izm).lower():
                                                        ed_izm_base = 20
                                                    elif compare_ratio_ei1 > 75:
                                                        ed_izm_base = ed_izm_id
                                                    elif compare_ratio_ei2 > 75:
                                                        ed_izm_base = ed_izm_id
                                                    else:
                                                        ed_izm_base = 46

                                                # Форма
                                                form = prodNm
                                                if prodNm != None:
                                                    prodNm = prodNm[:499]

                                                #Преобразование
                                                qty = qty.replace(',00000000000', '').replace(',', '.')
                                                price_spec = price_spec.replace(',', '.')
                                                summ = summ.replace(',', '.')
                                                try:
                                                    qty = int(qty)
                                                except:
                                                    pass

                                                if price_spec != None and summ == None:
                                                    try:
                                                        summ = float(price_spec) * qty
                                                    except:
                                                        pass

                                                #print(qty, type(qty))
                                                #print(prodtype, str(prodNm), form, str(okpd_code), okpd_name, price_spec, str(qty),summ,ed_izm,ed_izm_base, okpd_type)
                                                #sys.exit()

                                                conn1 = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                                       'Server=37.203.243.65\CURSORMAIN,49174;'
                                                                       'Database=CursorImport;'
                                                                       'UID=' + login_sql + ';'
                                                                       'PWD=' + password_sql + ';'
                                                                       'Trusted_Connection=no;')
                                                cursor1 = conn1.cursor()

                                                cursor1.execute(
                                                    "declare @i_notif int; Select @i_notif = Notif_ID FROM [CursorImport].[import].[Regional_Notif] where NotifNr = '" + str(notif) + "'; "
                                                                 "insert into [CursorImport].[import].[Regional_LotSpec]([Notif_ID], [ProdType], [ProdNm], [Form], [OKPD_code], [OKPD_name], [Price], [Amount], [Summ], [SYSDATE], Measure, Measure_base, okpd_type)"
                                                                 "values (@i_notif, ?, ?, ?, ?, ?, ?, convert(numeric(16,0),'"+str(qty)+"'), ?, getdate(), ?, ?, ?) ",
                                                    prodtype, str(prodNm), form, str(okpd_code), okpd_name, price_spec, #str(qty),
                                                    summ,ed_izm,ed_izm_base, okpd_type
                                                )
                                                if isDebug == False:
                                                    conn1.commit()
                                                conn1.close()

                                        isnotdone = False
                                        print('Done')
                                        loaded.append(notif)
                                else:
                                    break
                            else:
                                print('Уже загружено')
                except KeyboardInterrupt:
                    sys.exit()
                except:
                    print('Подключение не удалось')
                    traceback.print_exc()
                    try:
                        if notif not in loaded:  # Необходимо, если прокси слетает не завершив закачку
                            isnotdone = True
                    except:
                        traceback.print_exc()
                    if isProxy == False:
                        isnotdone = False

                    if isDebug == True:
                        traceback.print_exc()
                        try:
                            print(table1)
                        except:
                            print('no table')
            if isnotdone == False:
                break