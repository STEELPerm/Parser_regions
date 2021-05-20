import requests
import traceback
import json
import pyodbc
import time
from bs4 import BeautifulSoup
import datetime

import parser_utils

def load_orgs(login_sql, password_sql, isDebug=True, isProxy=True):
    # Достаем прокси
    global org_name
    df_proxies = parser_utils.get_proxies(login_sql, password_sql)

    # Достаем инфу о сайтах
    reg_sites_query = "SELECT id, isnull(Version,1) as version FROM [CursorImport].[import].[RegionalWebsites] " #where Host = 'zmo-new-webapi.rts-tender.ru'
    df_reg_site = parser_utils.select_query(reg_sites_query, login_sql, password_sql)

    for df_id, reg_site_id in df_reg_site.iterrows():
        reg_site_id0 = str(reg_site_id['id'])
        version = reg_site_id['version']
        isnotdone = True

        # STEEL проставляем организации в SQL через хранимку
        try:
            print('Проставляем организации')

            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                  'Server=37.203.243.65\CURSORMAIN,49174;'
                                  'Database=CursorImport;'
                                  'UID=' + login_sql + ';'
                                  'PWD=' + password_sql + ';'
                                  'Trusted_Connection=no;')
            cursor = conn.cursor()
            cursor.execute("exec [CursorImport].[import].[spRegions_FillOrg] " + reg_site_id0)
            if isDebug == False:
                conn.commit()
            conn.close()
            print('Проставили')
        except:
            # if isDebug == True:
            traceback.print_exc()
            print('Ошибка при апдейте организаций (spRegions_FillOrg)')

        if version == 1:
            for i in range(0, 10):
                while isnotdone == True:
                    proxy = ''
                    # try:
                    print('Попытка №' + str(i + 1))
                    if isProxy == True:
                        proxy = df_proxies.sample(1)['proxy'].reset_index(drop=True)[
                            0]  # Берем один рандомный прокси из DataFrame
                        print('Подключаюсь, используя прокси: ' + str(proxy))
                        prox = str(proxy).replace(' ', '')
                        proxies = {'http': 'http://' + prox, 'https': 'http://' + prox, }
                    try:
                        df_orgs_not_in_base_query = "SELECT distinct c.[RegSite_ID], c.[Customer_ID] FROM [CursorImport].[import].[RegionalCommon] c left join [CursorImport].[import].[RegionalOrgs] o on c.Customer_ID=o.Customer_ID where o.Customer_ID is null and c.Customer_ID is not null and c.RegSite_ID = " + reg_site_id0
                        df_orgs_not_in_base = parser_utils.select_query(df_orgs_not_in_base_query, login_sql, password_sql, 'CursorImport')

                        df_sites_query = "select * FROM [CursorImport].[import].[RegionalWebsites] where id = " + reg_site_id0
                        df_sites = parser_utils.select_query(df_sites_query, login_sql, password_sql, 'CursorImport')

                        i = 0
                        referer = df_sites['Referer'][i]
                        site = df_sites['descr'][i]
                        print('Сейчас обрабатывается: ', site)

                        for id0, row in df_orgs_not_in_base.iterrows():
                            customer = row['Customer_ID']
                            url = referer + 'Customer/ViewCustomerInfo?customerId=' + str(customer)
                            print(url)
                            headers = {
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                'Content-type': 'application/json; charset=UTF-8',
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                            }
                            time.sleep(2)
                            #print('proxy=',isProxy)
                            if isProxy == True:
                                try:
                                    r = requests.post(url, headers=headers, verify=False, proxies=proxies,
                                                      timeout=10)  # Пробуем подключиться
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout')
                                    isError = True
                            else:
                                try:
                                    r = requests.post(url, headers=headers, verify=False,timeout=50)
                                except requests.exceptions.Timeout as e:
                                    print('Connection timeout')
                                    isError = True

                            parsed_html = BeautifulSoup(r.text, features="html.parser")
                            try:
                                org_name = str(parsed_html).split('<td>Наименование</td>')[1].split('</tr>')[0].replace('\r',
                                                                                                                        '').replace(
                                    '\n', '').replace('</td>', '').replace('<td>', '')
                            except:
                                try:
                                    org_name = str(parsed_html).split('<td>Полное наименование</td>')[1].split('</tr>')[0].replace(
                                        '\r',
                                        '').replace(
                                        '\n', '').replace('</td>', '').replace('<td>', '')
                                except:
                                    pass
                            print('org_name=',org_name)
                            org_inn = \
                                str(parsed_html)[int(len(str(parsed_html)) / 2):].split('<td>ИНН</td>')[1].split('</tr>')[
                                    0].replace('\r', '').replace('\n', '').replace('</td>', '').replace('<td>',
                                                                                                        '')  # Делим строку на половину и находим инн
                            org_kpp = \
                                str(parsed_html)[int(len(str(parsed_html)) / 2):].split('<td>КПП</td>')[1].split('</tr>')[
                                    0].replace('\r', '').replace('\n', '').replace('</td>', '').replace('<td>', '')
                            try:
                                org_addr = str(parsed_html).split('<td>Адрес юридический</td>')[1].split('</tr>')[0].replace(
                                    '\r', '').replace('\n', '').replace('</td>', '').replace('<td>', '')
                            except:
                                try:
                                    org_addr = str(parsed_html).split('<td>Адрес места нахождения</td>')[1].split('</tr>')[
                                        0].replace(
                                        '\r', '').replace('\n', '').replace('</td>', '').replace('<td>', '')
                                except:
                                    pass

                            print('Закачиваем организацию: ', org_name, org_inn, org_kpp, org_addr)
                            #print('TYT')
                            #sys.exit()
                            conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                                  'Server=37.203.243.65\CURSORMAIN,49174;'
                                                  'Database=CursorImport;'
                                                  'UID='+login_sql+';'
                                                  'PWD='+password_sql+';'
                                                  'Trusted_Connection=no;')
                            cursor = conn.cursor()
                            cursor.execute(
                                "insert into [CursorImport].[import].[RegionalOrgs]([RegSite_ID], [Customer_id], [Customer_INN], [Customer_KPP], [Customer_Addr], [SYSDATE], Customer_name) "
                                "values (?, ?, ?, ?, ?, getdate(), ?) ",
                                str(i), str(customer), str(org_inn), str(org_kpp), org_addr, org_name)
                            if isDebug == False:
                                conn.commit()
                            conn.close()

                            time.sleep(0.5)

                        isnotdone = False
                    except:
                        #if isDebug == True:
                        traceback.print_exc()
                        print('Подключение через прокси не удалось')



                if isnotdone == False:
                    break