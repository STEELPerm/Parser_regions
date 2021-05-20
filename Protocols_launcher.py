import datetime
import sys
import time
import yagmail

import parser_utils
import Parser_protocols

sql_login_file = 'login_sql.txt'

Debug = False  # дебаг, если True - вкл, False - выкл
Proxy = False  # прокси, если True - вкл, False - выкл


if __name__ == '__main__':
    login_sql, password_sql = parser_utils.login(sql_login_file)
    if len(sys.argv) > 1:
        print('Парсим по номерам')
        args = []
        i = 1
        ln = len(sys.argv)
        while i < ln:
            arguments = sys.argv[i]
            args.append(arguments)
            i += 1
        Parser_protocols.get_protocols(login_sql, password_sql, isDebug=Debug, isProxy=Proxy, args=args)
    else:
        Parser_protocols.get_protocols(login_sql, password_sql, isDebug=Debug, isProxy=Proxy)