import requests
import datetime
import sys
import time
import yagmail

import parser_utils
import Regions_notifs
import Regions_initial
import Regions_orgs
import Notifs_2_base
import Parser_protocols

"""""
-- Author: Vlad Trubnikov
-- Create date: 19.08.2020
-- Description:	Парсинг региональных сайтов

-- Notes:
- При включенном дебаге не закачивание позиций - норма, т.к. commit для всех транзакций в базу отключен и запрос 
не может получить id закаченных извещений для привязки продуктов.

-- TODO:
- Даже если прокси выключено - он все равно будет писать, что подключается через него.
- При включении проксей, Notif_2_base.py может заливать неправильно привязанные позиции. 
Позиции от одного извещения могут оказаться у другого. ХЗ откуда это приходит
"""""

sql_login_file = 'login_sql.txt'
mail_login_file = 'login_mail.txt'
mails_file = 'mails_to_send.txt'
words_file = 'words.txt'

Debug = False  # дебаг, если True - вкл, False - выкл
Proxy = False  # прокси, если True - вкл, False - выкл

if __name__ == '__main__':
    # Приветствие
    DT = datetime.datetime.today().weekday()
    if DT == 2:
        print('It`s ' + str(datetime.datetime.today().strftime('%A')) + '                    ...my dudes..')
    else:
        print('It`s ' + str(datetime.datetime.today().strftime('%A')))

    startT = datetime.datetime.now()  # Время для отслеживания работы всего парсера
    startTime = datetime.datetime.now()  # Забираем сегодняшнюю дату из открытой библитеки
    t1 = startTime.strftime('%Y-%m-%d')  # Форматирование даты №1
    t2 = startTime.strftime('%Y%m%d')
    yesterdayTime = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterdayTime.strftime('%Y%m%d')

    login_sql, password_sql = parser_utils.login(sql_login_file)
    login_mail, password_mail = parser_utils.login(mail_login_file)

    mails = parser_utils.get_list_from_txt(mails_file)
    words_to_parse = parser_utils.get_list_from_txt(words_file)

    if len(sys.argv) > 1:  # Достаем номера через консоль (если они были даны)
        if str(sys.argv[1]).lower() == 'n':  # Если извещения
            print('Парсим номера извещений')
            word = None.exceptions
            args = []
            i = 2
            ln = len(sys.argv)
            while i < ln:
                arguments = sys.argv[i]
                args.append(arguments)
                i += 1
            Regions_notifs.load_notifs_to_import(login_sql, password_sql, yesterday, args_to_parse=args, isArgs=True, isDebug=Debug, isProxy=Proxy)
        elif len(sys.argv[1]) == 8:
            date_start = sys.argv[1]
            print('Парсим по дате: ' + str(date_start))
            for word in words_to_parse:
                word = word.replace('?', '')
                print(word)
                Regions_initial.find_notifs(login_sql, password_sql, word, date_start, isDebug=Debug, isProxy=Proxy)
                Regions_orgs.load_orgs(login_sql, password_sql, isDebug=Debug, isProxy=Proxy)
                Regions_notifs.load_notifs_to_import(login_sql, password_sql, date_start, isDebug=Debug, isProxy=Proxy)
        else:
            word = None
            args = []
            i = 1
            ln = len(sys.argv)
            while i < ln:
                arguments = sys.argv[i]
                args.append(arguments)
                i += 1
            Regions_notifs.load_notifs_to_import(login_sql, password_sql, yesterday, args_to_parse=args, isArgs=True, isDebug=Debug, isProxy=Proxy)
    else:  # Достаем номера через поиск
        for word in words_to_parse:
            word = word.replace('?', '')
            pages_to_search = 3  # Страниц для поиска, иначе даже при установлении даты он качает всё.
            print(word)
            Regions_initial.find_notifs(login_sql, password_sql, word, yesterday, pages=pages_to_search, isDebug=Debug, isProxy=Proxy)
            Regions_orgs.load_orgs(login_sql, password_sql, isDebug=Debug, isProxy=Proxy)
            Regions_notifs.load_notifs_to_import(login_sql, password_sql, yesterday, isDebug=Debug, isProxy=Proxy)

            #print('TYT2')
            #sys.exit()

    Notifs_2_base.load_to_base(login_sql, password_sql, isDebug=Debug)

    # Отправка статистики и отчетов
    end = str(datetime.datetime.now() - startT)
    statistics_query = "SELECT [Tender_ID] FROM [Cursor].[dbo].[Tender] where LEN(NotifNr) = 7 and SYSDATE >= '" + t2 + "'"
    stats_list = parser_utils.select_query(statistics_query, login_sql, password_sql, isList=True)

    contents_st = [
        'Парсинг Региональных сайтов завершен. Всего времени на выполнение: ' + end + '\n\nВсего строк: ' + str(len(stats_list))]
    print('Sending mail ...')
    yag = yagmail.SMTP(login_mail, password_mail, host='smtp.yandex.ru')
    yag.send(mails, 'Завершение парсинга Регионалных сайтов', contents_st)

    print('All Done!')
    print(end)
    time.sleep(3)
    sys.exit()
