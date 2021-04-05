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
    print('Org_ID in database:', list_org_check)
    if list_org_check:
        response = 'Организация с таким ИНН уже есть в базе'
    else:
        try:
            info = get_KPP(inn)
            # Проставляем ZIP
            if parser_utils.is_number(info['Adr'][0][:6]) == True:
                info['ZIP'] = info['Adr'][0][:6]
            else:
                info['ZIP'] = None

            # Проставляем код региона
            reg_query = 'SELECT [RegNm], [RegCode] FROM [Cursor].[dbo].[Region]'
            df_regs = parser_utils.select_query(reg_query, login_sql, password_sql)

            i = 0
            len_regs = len(df_regs)
            id = []
            while i < len_regs:
                lol = df_regs['RegNm'][i].find(info['reg'][0])
                id.append(lol)
                i += 1

            m = max(id)
            m1 = [i for i, j in enumerate(id) if j == m]
            if m != -1:
                info['reg_id'] = int(df_regs['RegCode'][m1])
            else:
                info['reg_id'] = 77

            # Нужно еще достать тип организации по ее названию через хранимку
            connn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                 'Server=37.203.243.65\CURSORMAIN,49174;'
                                 'Database=Cursor;'
                                 'UID='+login_sql+';'
                                 'PWD='+password_sql+';'
                                 'Trusted_Connection=no;')
            cursor0 = connn.cursor()
            cursor0.execute("SELECT dbo.udf_GetTypeOrg('" + str(info['FName'][0]) + "')")
            con1 = []
            for row in cursor0:
                con1.append(row[0])
            connn.close()

            try:
                kpp0 = str(int(info['KPP'][0]))
            except:
                kpp0 = None

            if info['Contact'][0] == 'null':
                cnt0 = None
            else:
                cnt0 = str(info['Contact'][0])

            if info['okved'][0] == 'null':
                okved = None
            else:
                okved = str(info['okved'][0])

            form_fin = str(info['FormNm'][0])
            try:
                form_fin = form_fin.replace('Российской Федерации', 'РФ')
            except:
                try:
                    form_fin = form_fin.replace('Сельскохозяйственный', 'Сх')
                except:
                    pass

            if str(info['SName'][0]) == 'None':
                Sname = str(info['FName'][0])
            else:
                Sname = str(info['SName'][0])

            # Org
            con00 = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                   'Server=37.203.243.65\CURSORMAIN,49174;'
                                   'Database=Cursor;'
                                   'UID='+login_sql+';'
                                   'PWD='+password_sql+';'
                                   'Trusted_Connection=no;')
            cursor00 = con00.cursor()
            cursor00.execute(
                "insert into [Cursor].[dbo].[Org](OrgNm, OrgNmS, FormNm, Cntr_ID, Addr1, Contact1, email, phone, RegCode, SYSDATE, UserID, INN, ZIP, isAutomate, KPP, ConsigneeType_FK, OGRN, isActive, OKVED)"
                "values (?, ?, ?, '643', ?, ?, ?, ?, ?, GETDATE(), 'FDA506B0-2F4C-49F4-864F-836731C63391', ?, ?, '1', ?, ?, ?, ?, ?) ",
                str(info['FName'][0]), Sname, form_fin, str(info['Adr'][0]),
                cnt0, str(info['email'][0]), str(info['phone'][0]), str(info['reg_id'][0]),
                str(info['inn'][0]), str(info['ZIP'][0]), kpp0, con1[0], str(info['ogrn'][0])[:12], str(info['status'][0]),
                okved)
            con00.commit()
            con00.close()

            print('Done')
            response = 'Организация ' + Sname + ' добавлена'
        except:
            print('Error')
            response = 'В ИНН была допущена ошибка или такой организации нет в ЕГРЮЛ'
    return response


#get_KPP('0542019185')

create('7722270954', 'v_trubnikov', 'S7J2Hw9cj9a9')