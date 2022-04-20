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
    print(r0)  # Вывод результата запроса
    # Фильтруем КПП из массива
    ln = len(r0['suggestions'])
    #print(ln)

def A ():
    G = 1000
    print('XRU')
    return G

#get_KPP('5610048595')

#tenddt = '11.03.2021'

#print(tenddt)

#global G
# if __name__ == '__main__':
#     k = A()
#
# print(k)

