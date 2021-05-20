import requests
import json
import pyodbc
import time
import datetime
import sys
import traceback
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import parser_utils

def find_notifs(login_sql, password_sql, word_to_parse, date_to_parse, pages=None, isDebug=True, isProxy=True):
    # Достаем прокси
    df_proxies = parser_utils.get_proxies(login_sql, password_sql)

    # Достаем инфу о сайтах
    reg_sites_query = "SELECT id, isnull(version,1) as version FROM [CursorImport].[import].[RegionalWebsites]" # where Host = 'zmo-new-webapi.rts-tender.ru'
    df_reg_site = parser_utils.select_query(reg_sites_query, login_sql, password_sql)

    for df_id, reg_site_id in df_reg_site.iterrows():
        reg_site_id0 = str(reg_site_id['id'])
        version = reg_site_id['version']
        isnotdone = True
        for i in range(0, 3): #10
            while isnotdone == True:
                # try:
                #time.sleep(2)
                time.sleep(1)
                proxy = ''
                print('Попытка №' + str(i + 1))
                if isProxy == True:
                    proxy = df_proxies.sample(1)['proxy'].reset_index(drop=True)[
                        0]  # Берем один рандомный прокси из DataFrame
                    print('Подключаюсь, используя прокси: ' + str(proxy))
                    prox = str(proxy).replace(' ', '')
                    proxies = {'http': 'http://' + prox, 'https': 'http://' + prox, }
                isError = False
                try:
                    df_tenders_in_base_query = "SELECT [RegSite_ID], [Local_Notif_ID] FROM [CursorImport].[import].[RegionalCommon] where RegSite_ID = " + reg_site_id0
                    df_tenders_in_base = parser_utils.select_query(df_tenders_in_base_query, login_sql, password_sql, 'CursorImport')

                    df_sites_query = "select * FROM [CursorImport].[import].[RegionalWebsites] where id = " + reg_site_id0
                    df_sites = parser_utils.select_query(df_sites_query, login_sql, password_sql, 'CursorImport')

                    i = 0
                    url = df_sites['API'][i]
                    host = df_sites['Host'][i]
                    tenant_id = df_sites['Tenant_ID'][i]
                    referer = df_sites['Referer'][i]
                    refererAdd = df_sites['RefererAdd'][i]
                    if refererAdd is not None:
                        refererAll = referer + refererAdd
                    else:
                        refererAll = referer
                    site = df_sites['descr'][i]
                    #print(referer)
                    print('Сейчас обрабатывается: ', site)

                    headers = {'Accept': '*/*', 'Host': host, 'Connection': 'keep-alive',
                               'XXX-TenantId-Header': tenant_id, 'Referer': refererAll,
                               'Content-type': 'application/json; charset=UTF-8',
                               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'}
                    i0 = 1
                    TotalPages = 2
                    while i0 < TotalPages + 1:
                        payload = {"page": i0, "itemsPerPage": 50, "tradeName": str(word_to_parse),
                                   "filterDateFrom": str(date_to_parse) + "T00:00:00.000Z", "UsedClassificatorType": 5,
                                   "TradeSearchType": 50}
                        time.sleep(2)
                        #print(isProxy)
                        if isProxy == True:
                            try:
                                r = requests.post(url, data=json.dumps(payload), headers=headers, verify=False,
                                                  proxies=proxies, timeout=10)  # Пробуем подключиться
                            except requests.exceptions.Timeout as e:
                                print('Connection timeout')
                                isError = True
                        else:
                            try:
                                r = requests.post(url, data=json.dumps(payload), headers=headers, verify=False, timeout=50)
                            except requests.exceptions.Timeout as e:
                                print('Connection timeout')
                                isError = True
                                i0 = TotalPages + 1

                        try:
                            if isError == False:
                                r0 = r.json()
                                print(r0, url)
                                if pages == None:
                                    if r0['totalpages'] > TotalPages:  # Достаем полный объем страниц и заменяем выше заданную переменную
                                        TotalPages = r0['totalpages']
                                    else:
                                        pass
                                else:
                                    TotalPages = pages

                                for data in r0['invdata']:
                                    reg_site_id = reg_site_id0
                                    local_notif_id = str(data['Id'])
                                    status = str(data['TradeStateName'])
                                    publdt = str(data['PublicationDate'])
                                    try:
                                        apps = str(data['CountApplications'])
                                    except:
                                        try:
                                            apps = str(data['ApplicationsCount'])
                                        except:
                                            apps = 0

                                    try:
                                        customer_id = str(data['CustomerId'])
                                    except:
                                        customer_id = None


                                    df_tenders_in_base2 = df_tenders_in_base['Local_Notif_ID'][
                                        df_tenders_in_base['RegSite_ID'].isin([reg_site_id])].values.tolist()

                                    if int(local_notif_id) not in df_tenders_in_base2:
                                        # STEEL взять организацию со страницы
                                        try:
                                            # customer_id = None
                                            url1 = referer + '/Trade/ViewTrade?id=' + str(local_notif_id)
                                            # print(url1)
                                            headers1 = {
                                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                'Content-type': 'application/json; charset=UTF-8',
                                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                                            }
                                            # time.sleep(2)
                                            # print('proxy=', isProxy)
                                            if isProxy == True:
                                                try:
                                                    r1 = requests.post(url1, headers=headers1, verify=False,
                                                                       proxies=proxies,
                                                                       timeout=10)  # Пробуем подключиться
                                                except requests.exceptions.Timeout as e:
                                                    print('Connection timeout')
                                                    isError = True
                                            else:
                                                try:
                                                    r1 = requests.post(url1, headers=headers1, verify=False,timeout=50)
                                                except requests.exceptions.Timeout as e:
                                                    print('Connection timeout')
                                                    isError = True

                                            parsed_html1 = BeautifulSoup(r1.text, features="html.parser")

                                            # Новый формат сайта: api.market.mosreg.ru (Московская область) version = 2
                                            if version == 1:
                                                org_name = \
                                                    str(parsed_html1).split(
                                                        '<label for="CustomerFullName">Полное наименование</label>')[
                                                        1].split('</tr>')[
                                                        0].replace('\r',
                                                                   '').replace(
                                                        '\n', '').replace('</td>', '').replace('<td>', '')

                                                org_inn = \
                                                    str(parsed_html1)[int(len(str(parsed_html1)) / 2):].split(
                                                        '<label for="CustomerInn">ИНН</label>')[
                                                        1].split('</tr>')[
                                                        0].replace('\r', '').replace('\n', '').replace('</td>',
                                                                                                       '').replace(
                                                        '<td>',
                                                        '')  # Делим строку на половину и находим инн

                                                org_addr = \
                                                    str(parsed_html1).split(
                                                        '<label for="CustomerPhysAddress">Адрес места нахождения</label>')[
                                                        1].split('</tr>')[
                                                        0].replace(
                                                        '\r', '').replace('\n', '').replace('</td>', '').replace('<td>',
                                                                                                                 '')
                                            elif version == 2:
                                                items = parsed_html1.find_all('div',
                                                                              class_='informationAboutCustomer__data-infoBlock infoBlock')

                                                for item in items:
                                                    item2 = item.find('span', class_='infoBlock__label').get_text(
                                                        strip=True)

                                                    if item2 == 'Полное наименование:':
                                                        org_name = item.find('a', class_='infoBlock__link').get_text(
                                                            strip=True).lstrip().rstrip()
                                                    if item2 == 'ИНН:':
                                                        org_inn = item.find('p', class_='infoBlock__text').get_text(
                                                            strip=True).lstrip().rstrip()
                                                    if item2 == 'Адрес места нахождения:':
                                                        org_addr = item.find('p', class_='infoBlock__text').get_text(
                                                            strip=True).lstrip().rstrip()

                                            # print('org_name=('+ org_name+')')
                                            # print('org_inn=('+ org_inn+')')
                                            # print('org_addr=('+ org_addr+')')
                                            # sys.exit()
                                        except:
                                            pass
                                            # customer_id = None
                                            print('Ошибка при распарсивании организации со страницы: ') + url1
                                            isnotdone = False
                                            isError = True
                                            traceback.print_exc()
                                            # sys.exit()

                                        print('Закачиваем извещение с ID: ', local_notif_id)
                                        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                              'Server=37.203.243.65\CURSORMAIN,49174;'
                                                              'Database=CursorImport;'
                                                              'UID='+login_sql+';'
                                                              'PWD='+password_sql+';'
                                                              'Trusted_Connection=no;')
                                        cursor = conn.cursor()
                                        cursor.execute(
                                            "insert into [CursorImport].[import].[RegionalCommon]([RegSite_ID], [Local_Notif_ID], [Status], [PublDt], [CountApplications], [Customer_ID], [SYSDATE], [isImported], [Customer_INN], [Customer_Name], [Customer_Addr]) "
                                            "values (?, ?, ?, ?, ?, ?, GETDATE(), '0', ?, ?, ?) ",
                                            reg_site_id, local_notif_id, status, publdt, apps, customer_id, org_inn, org_name, org_addr)
                                        if isDebug == False:
                                            conn.commit()
                                        conn.close()
                                    else:
                                        print('Извещение ', local_notif_id, ' есть в базе')

                                i0 += 1
                            else:
                                isnotdone = False
                                break
                        except:
                            print('Ошибка при работе с json')
                            isnotdone = False
                            isError = True
                            i0 = TotalPages + 1

                        isnotdone = False
                except:
                    print('Подключение через прокси не удалось')
                    traceback.print_exc()
                    if isDebug == True:
                        traceback.print_exc()