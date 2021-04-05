import pandas as pd
import requests
import json
import pyodbc

import parser_utils

API_KEY = parser_utils.get_list_from_txt('api.txt')[0]
BASE_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"
num1 = 10  # Число цифр в ИНН
cols = ['KPP', 'FName', 'SName', 'Adr', 'FormNm', 'Contact', 'inn', 'ogrn', 'phone', 'email', 'reg', 'status', 'okved']


def get_KPP(query):
    # Full request to the API // Запрос данных по фирме
    url = BASE_URL.format("party")
    headers = {"Authorization": "Token {}".format(API_KEY), "Content-Type": "application/json"}
    data = {"query": query}
    r = requests.post(url, data=json.dumps(data), headers=headers)
    r0 = r.json() # Resulting json
    # print(r0)  # Вывод результата запроса
    # Фильтруем КПП из массива
    ln = len(r0['suggestions'])
    br1 = []
    nm1 = []
    nms = []
    adr = []
    opf = []
    cnt = []
    inn = []
    ogrn = []
    phone = []
    email = []
    reg_full = []
    status = []
    x_okv = []

    i = 0
    while i < ln:
        try:
            x1 = r0['suggestions'][i]['data']['kpp']
        except:
            x1 = 'null'
        x2 = r0['suggestions'][i]['data']['name']['full_with_opf']
        x21 = r0['suggestions'][i]['data']['name']['short_with_opf']
        print(x21)
        x3 = r0['suggestions'][i]['data']['address']['unrestricted_value']
        x4 = r0['suggestions'][i]['data']['opf']['short']
        try:
            x5 = r0['suggestions'][i]['data']['management']['name']
        except:
            x5 = 'null'
        x6 = r0['suggestions'][i]['data']['inn']
        x7 = r0['suggestions'][i]['data']['ogrn']
        x8 = r0['suggestions'][i]['data']['phones']
        x9 = r0['suggestions'][i]['data']['emails']

        x10 = r0['suggestions'][i]['data']['address']['data']['region']
        x11 = r0['suggestions'][i]['data']['address']['data']['region_type_full']
        x12 = x10 + ' ' + x11

        x_stat = r0['suggestions'][i]['data']['state']['status']
        if x_stat == 'LIQUIDATED':
            x_stat = 0
        else:
            x_stat = 1

        x_okv1 = r0['suggestions'][i]['data']['okved']

        br1.append(x1)
        nm1.append(x2)
        nms.append(x21)
        adr.append(x3)
        opf.append(x4)
        cnt.append(x5)
        inn.append(x6)
        ogrn.append(x7)
        phone.append(x8)
        email.append(x9)
        reg_full.append(x10)
        status.append(x_stat)
        x_okv.append(x_okv1)

        i += 1
    else:
        d = pd.DataFrame(data=br1)
        n = pd.DataFrame(data=nm1)
        ns = pd.DataFrame(data=nms)
        a = pd.DataFrame(data=adr)
        opf = pd.DataFrame(data=opf)
        cnt = pd.DataFrame(data=cnt)
        inn = pd.DataFrame(data=inn)
        ogrn = pd.DataFrame(data=ogrn)
        phone = pd.DataFrame(data=phone)
        email = pd.DataFrame(data=email)
        reg_full = pd.DataFrame(data=reg_full)
        status = pd.DataFrame(data=status)
        x_okv = pd.DataFrame(data=x_okv)

        info = pd.concat([d, n, ns, a, opf, cnt, inn, ogrn, phone, email, reg_full, status, x_okv], axis=1).reset_index(drop=True)
        info.columns = cols
    return info


def create(inn, login_sql, password_sql):
    df_org_check_query = "SELECT [Org_ID] FROM [Cursor].[dbo].[Org] where INN = '" + str(inn) + "'"
    list_org_check = parser_utils.select_query(df_org_check_query, login_sql, password_sql, isList=True)
    #print('Org_ID in database:', list_org_check)
    #if list_org_check:
    #    response = 'Организация с таким ИНН уже есть в базе'
    #else:
    try:
      info = get_KPP(inn)
      # Проставляем ZIP
      if parser_utils.is_number(info['Adr'][0][:6]) == True:
          info['ZIP'] = info['Adr'][0][:6]
      else:
          info['ZIP'] = None

      print(info.head(5))

      response = 'Организация  добавлена'

    except:
        print('Error')
        response = 'В ИНН была допущена ошибка или такой организации нет в ЕГРЮЛ'
    return response

create('2983013310', 'v_trubnikov', 'S7J2Hw9cj9a9')