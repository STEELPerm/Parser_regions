import pandas as pd
import pyodbc
import time
from fuzzywuzzy import fuzz
import numpy as np

import parser_utils

def load_to_base(login_sql, password_sql, isDebug=True):
    # Добываем список МНН из базы
    conn_med = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=37.203.243.65\CURSORMAIN,49174;'
                              'Database=Cursor;'
                              'UID='+login_sql+';'
                              'PWD='+password_sql+';'
                              'Trusted_Connection=no;')
    cursor = conn_med.cursor()
    cursor.execute(
        "SELECT Mnn_Name, Tn_Name, Innnx, TradeNmNx, MtdID_Output_WithOutTn, MtdID_Output_WithTn FROM [Cursor].[dod].[DataForResolver]")  # Забираем список прокси из базы

    meds = []
    meds_tns = []
    mnn_ids = []
    tn_ids = []
    mtd_withs = []
    mtd_withouts = []
    for row in cursor:
        meds.append(row[0])
        meds_tns.append(row[1])
        mnn_ids.append(row[2])
        tn_ids.append(row[3])
        mtd_withs.append(row[4])
        mtd_withouts.append(row[5])
    conn_med.close()

    notifs_import_query1 = "SELECT rn.*, month(dateadd(month,1,rn.TendDt)) as PlanT_ID, year(dateadd(month,1,rn.TendDt)) as PlanTYear, dateadd(month,1,rn.TendDt) as SupplyDt FROM [CursorImport].[import].[Regional_Notif] rn(nolock) left join [Cursor].dbo.Tender t (nolock) on rn.NotifNr=t.NotifNr left join [Cursor].log.DeletedTenders dt (nolock) on rn.NotifNr=dt.NotifNr where t.Tender_ID is null and (dt.id is null or (dt.id is null and rn.SYSDATE > dt.DDeleting))"
    notifs_import = parser_utils.select_query(notifs_import_query1, login_sql, password_sql)

    for index0, row in notifs_import.iterrows():
        time.sleep(0.25)
        print(row['NotifNr'])
        id_notif = row['Notif_ID']

        # Забираем лот из импорта
        #lot_import_query = "SELECT * FROM [CursorImport].[import].[Regional_Lot] where Notif_ID = " + str(id_notif)
        lot_import_query = "SELECT *, isnull((select RegCode from [cursor].dbo.Org where Org_ID = (select Org_ID from [CursorImport].[import].[Regional_Notif] where Notif_ID=L.Notif_ID)),L.Reg_ID) as Reg_ID_Cust FROM [CursorImport].[import].[Regional_Lot] L where Notif_ID = " + str(id_notif)
        lot_import = parser_utils.select_query(lot_import_query, login_sql, password_sql)

        # Проставляем переменные для закачки в базу
        notifnr = row['NotifNr']
        publdt = row['PublDt']
        url = row['Url']
        cust_id = row['Org_ID']
        claimreg = row['ClaimReglament']
        tenddt = row['TendDt']
        tendnm = row['TendNm']
        tenderprice = row['TenderPrice']
        condition = row['Condition']
        descr = row['Descr']
        tendtend = row['TendEndDt']
        contractplanned = row['ContractPlannedDt']
        fz_id = row['FZ_ID_import']

        #STEEL от 19.04.2021 Заполнение периода и года поставки: берем месяц, следующий (+1) из даты проведения.
        PlanT_ID = row['PlanT_ID']
        PlanTYear = row['PlanTYear']
        SupplyDt = row['SupplyDt']

        lotnm = lot_import['LotNm'][0]
        pricestart = lot_import['PriceStart'][0]
        deliveryplace = lot_import['DeliveryPlace'][0]

        #STEEL от 31.05.2021 взять регион из организации
        #regid = lot_import['Reg_ID'][0]
        regid = lot_import['Reg_ID_Cust'][0]

        statusid = lot_import['LotStatus_ID'][0]
        if statusid == None:
            statusid = 5
        deliverydtdescr = lot_import['DeliveryDtSite'][0]

        if str(tenderprice) == 'nan':
            tenderprice = None

        auction_type = '41'

        # Забираем данные по заказчику из базы
        cust_query = "SELECT OrgNM, Addr1 FROM [Cursor].[dbo].[Org] where Org_ID = " + str(cust_id)
        cust_info = parser_utils.select_query(cust_query, login_sql, password_sql)
        if cust_info.empty == False:
            custnm = cust_info['OrgNM'][0]
            custaddr = cust_info['Addr1'][0]
        else:
            custnm = None
            custaddr = None

        # ИЗВЕЩЕНИЕ
        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=37.203.243.65\CURSORMAIN,49174;'
                              'Database=Cursor;'
                              'UID='+login_sql+';'
                              'PWD='+password_sql+';'
                              'Trusted_Connection=no;')
        cursor = conn.cursor()
        cursor.execute(
            "insert into [Cursor].[dbo].[Tender](ProcDt, PublDt, SrcInf, NotifNr, TendNr, StatusT_ID, FormT_ID, Reg_ID, ProviderT_ID, TenderDocReglament, Cust_ID, ClaimReglament, ClaimDtBeg, ClaimDtEnd, TendDt, TendNm, TenderPrice, Lot, ContrPrice, ContrPriceMax, PaymentReglament, ObespPerc, ObespSum, IsDraft, SYSDATE, LotCount, SpecCount, ImportType, isAutomate, UserID, OwnerID, Budget, BudgetProg_FK, Planned, FZ_ID) "
            "values (getdate(), ?, ?, ?, 'Б/н', ?, ?, ?, ?, 'Средства учреждений', ?, 'rts-tender.ru', ?, ?, ?, ?, ?, '1', 0.0, 0.0, ?, 0.0, 0.0, 0, getdate(), 1, 0, 18, 1, 'FDA506B0-2F4C-49F4-864F-836731C63391', 'FDA506B0-2F4C-49F4-864F-836731C63391', 'U', 7, 0, ?) ",
            publdt, url, notifnr, int(statusid), auction_type, int(regid), cust_id, cust_id, tenddt, tendtend, tendtend, #tenddt, #STEEL от 10.08.2021 дата проведения - это "Дата окончания подачи предложений:" с сайта
            tendnm, tenderprice, condition, fz_id)
        if isDebug == False:
            conn.commit()
        conn.close()

        # ЛОТ
        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=37.203.243.65\CURSORMAIN,49174;'
                              'Database=Cursor;'
                              'UID='+login_sql+';'
                              'PWD='+password_sql+';'
                              'Trusted_Connection=no;')
        cursor = conn.cursor()
        cursor.execute(
            "declare @tender_id int; Select @tender_id = Tender_id FROM [Cursor].[dbo].[Tender] where NotifNr = '" + str(
                notifnr) + "'; "
                           "insert into [Cursor].[dbo].[Lot](Tender_ID, Pos, LotNr, LotNm, PriceStart, Reg_ID, ConsigneeNm, ConsigneeInfo, SYSDATE, isAutomate, isAutomateWinner, LotStatus, Consignee_ID, UserID, OwnerID, ClaimObesp, ContrObesp, SupplyDt, PlanT_ID, PlanTYear) "
                           "values (@tender_id, 1, 1, ?, ?, ?, ?, ?, getdate(), 1, 0, ?, ?, 'FDA506B0-2F4C-49F4-864F-836731C63391', 'FDA506B0-2F4C-49F4-864F-836731C63391', 0.0, 0.0, ?, ?, ?) ",
            lotnm, pricestart, int(regid), custnm, custaddr, int(statusid), cust_id, SupplyDt, PlanT_ID, PlanTYear)
        if isDebug == False:
            conn.commit()
        conn.close()

        # ПОЗИЦИИ
        # STEEL от 15.03.2021 На собрании решили, что будет ручной ввод (таблицу LotSpec заполнять не надо). Можно needls вынести в настройки.
        needls = False
        if needls == True:
            pos_import_query = "SELECT * FROM [CursorImport].[import].[Regional_LotSpec] where isnull(havedocs,0)=0 and Notif_ID = " + str(id_notif)
            pos_import = parser_utils.select_query(pos_import_query, login_sql, password_sql)
            if pos_import.empty == False:
                i_pos = 1
                for index_pos, row_pos in pos_import.iterrows():
                    time.sleep(0.25)

                    prodtype = row_pos['ProdType']
                    prodnm = row_pos['ProdNm']
                    form_pos = row_pos['Form']
                    price_pos = row_pos['Price']
                    amount_pos = row_pos['Amount']
                    sum_pos = row_pos['Summ']
                    ed_izm = row_pos['Measure_base']

                    # Сверяем очищенный МНН из Регионов с МНН из базы
                    mnn_pos = None
                    tn_pos = None
                    mtd_pos = None
                    cleared_prodnm = prodnm

                    # Очистка названия МНН
                    if ' (' in prodnm:
                        cleared_prodnm = prodnm.split(' (')[0]
                    elif ' таб' in prodnm:
                        cleared_prodnm = prodnm.split(' таб')[0]
                    elif ' N' in prodnm:
                        cleared_prodnm = prodnm.split(' N')[0]
                    elif ' №' in prodnm:
                        cleared_prodnm = prodnm.split(' №')[0]
                    elif ' капс' in prodnm:
                        cleared_prodnm = prodnm.split(' капс')[0]
                    elif '/' in prodnm:
                        cleared_prodnm = prodnm.split('/')[0]
                    elif ' аэр.' in prodnm:
                        cleared_prodnm = prodnm.split(' аэр.')[0]
                    elif ' амп.' in prodnm:
                        cleared_prodnm = prodnm.split(' амп.')[0]
                    elif ' кап.' in prodnm:
                        cleared_prodnm = prodnm.split(' кап.')[0]
                    elif ' п/в' in prodnm:
                        cleared_prodnm = prodnm.split(' п/в')[0]

                    # Сверяем очищенный МНН из Березки с МНН из базы
                    isFound = False

                    for med, tn, mnn_id, tn_id, mtd_wo, mtd_w in zip(meds, meds_tns, mnn_ids, tn_ids, mtd_withouts,
                                                                     mtd_withs):
                        if isFound == False:
                            # compare_meds = fuzz.token_set_ratio(cleared_prodnm.upper(), str(med).upper())
                            # compare_meds_tn = fuzz.token_set_ratio(cleared_prodnm.upper(), str(tn).upper())

                            if cleared_prodnm.upper().replace(' ', '') == str(med).upper().replace(' ', '') and \
                                    cleared_prodnm.upper().replace(' ', '') == str(tn).upper().replace(' ', ''):
                                print('МНН и ТН 1: ', cleared_prodnm, '||', str(med), '||', str(tn))
                                mnn_pos = mnn_id
                                tn_pos = tn_id
                                if tn == '~':
                                    mtd_pos = mtd_wo
                                else:
                                    mtd_pos = mtd_w
                                prodtype = 'L'
                                isFound = True

                            elif cleared_prodnm.upper().replace(' ', '') == str(tn).upper().replace(' ',
                                                                                                    '') and '~' not in str(
                                    med):
                                print('МНН и ТН 2: ', cleared_prodnm, '||', str(med), '||', str(tn))
                                mnn_pos = mnn_id
                                tn_pos = tn_id
                                mtd_pos = mtd_w
                                prodtype = 'L'
                                isFound = True


                    if isFound == False:
                        print('Совпадений не найдено. Ищем только МНН и ТН')
                        # Собираем мнн и тн с ~ в два разных раздела листов
                        meds2 = []
                        meds_tns2 = []
                        mnn_ids2 = []
                        tn_ids2 = []
                        mtd_withouts2 = []
                        mtd_withs2 = []

                        meds3 = []
                        meds_tns3 = []
                        mnn_ids3 = []
                        tn_ids3 = []
                        mtd_withouts3 = []
                        mtd_withs3 = []
                        for med, tn, mnn_id, tn_id, mtd_wo, mtd_w in zip(meds, meds_tns, mnn_ids, tn_ids, mtd_withouts,
                                                                         mtd_withs):
                            if '~' in str(tn):
                                meds2.append(med)
                                meds_tns2.append(tn)
                                mnn_ids2.append(mnn_id)
                                tn_ids2.append(tn_id)
                                mtd_withouts2.append(mtd_wo)
                                mtd_withs2.append(mtd_w)
                            elif '~' in str(med):
                                meds3.append(med)
                                meds_tns3.append(tn)
                                mnn_ids3.append(mnn_id)
                                tn_ids3.append(tn_id)
                                mtd_withouts3.append(mtd_wo)
                                mtd_withs3.append(mtd_w)

                        # Сравниваем эти разделы, там где есть наименование
                        # ТН
                        if isFound == False:
                            for med3, tn3, mnn_id3, tn_id3, mtd_wo3, mtd_w3 in zip(meds3, meds_tns3, mnn_ids3, tn_ids3,
                                                                                   mtd_withouts3, mtd_withs3):
                                if isFound == False:
                                    # compare_meds_tn = fuzz.partial_ratio(df_pos['лекар'].iloc[p1].upper().replace(' ',''), str(tn3).upper().replace(' ',''))

                                    if cleared_prodnm.upper() == str(tn3).upper():
                                        print('ТН: ', cleared_prodnm, str(tn3))
                                        mnn_pos = mnn_id3
                                        tn_pos = tn_id3
                                        mtd_pos = mtd_w3
                                        prodtype = 'L'
                                        isFound = True
                        # МНН
                        for med2, tn2, mnn_id2, tn_id2, mtd_wo2, mtd_w2 in zip(meds2, meds_tns2, mnn_ids2, tn_ids2,
                                                                               mtd_withouts2, mtd_withs2):
                            if isFound == False:
                                # compare_meds = fuzz.partial_ratio(df_pos['лекар'].iloc[p1].upper().replace(' ',''), str(med2).upper().replace(' ',''))

                                if cleared_prodnm.upper() == str(med2).upper():
                                    print('МНН1: ', cleared_prodnm, str(med2), str(tn2))
                                    mnn_pos = mnn_id2
                                    mtd_pos = mtd_wo2
                                    prodtype = 'L'
                                    isFound = True

                        # Если всё еще не нашли
                        if isFound == False:  # ТН
                            for med3, tn3, mnn_id3, tn_id3, mtd_wo3, mtd_w3 in zip(meds, meds_tns, mnn_ids, tn_ids,
                                                                                   mtd_withouts, mtd_withs):
                                if isFound == False:
                                    compare_meds_tn = fuzz.token_set_ratio(cleared_prodnm.upper(),
                                                                           str(tn3).upper())

                                    if compare_meds_tn > 99:
                                        print('ТН: ', cleared_prodnm, str(tn3))
                                        mnn_pos = mnn_id3
                                        tn_pos = tn_id3
                                        mtd_pos = mtd_w3
                                        prodtype = 'L'
                                        isFound = True

                        if isFound == False:
                            # Ищем только по МНН
                            for med2, tn2, mnn_id2, tn_id2, mtd_wo2, mtd_w2 in zip(meds, meds_tns, mnn_ids, tn_ids,
                                                                                   mtd_withouts, mtd_withs):
                                if isFound == False:
                                    # compare_meds = fuzz.partial_ratio(cleared_prodnm.upper().replace(' ',''), str(med2).upper().replace(' ',''))

                                    if cleared_prodnm.upper() == str(med2).upper():
                                        print('МНН только: ', cleared_prodnm, str(med2), str(tn2))
                                        mnn_pos = mnn_id2
                                        mtd_pos = mtd_wo2
                                        prodtype = 'L'
                                        isFound = True

                        if isFound == False:  # МНН сравнение
                            # Ищем только по МНН
                            for med2, tn2, mnn_id2, tn_id2, mtd_wo2, mtd_w2 in zip(meds, meds_tns, mnn_ids, tn_ids,
                                                                                   mtd_withouts,
                                                                                   mtd_withs):
                                if isFound == False:
                                    compare_meds = fuzz.partial_ratio(cleared_prodnm.upper(),
                                                                      str(med2).upper())

                                    if compare_meds > 99:
                                        print('МНН 3: ', cleared_prodnm, str(med2), str(tn2))
                                        mnn_pos = mnn_id2
                                        mtd_pos = mtd_wo2
                                        prodtype = 'L'
                                        isFound = True
                    if isFound == False:
                        prodtype = '-'

                    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                          'Server=37.203.243.65\CURSORMAIN,49174;'
                                          'Database=Cursor;'
                                          'UID='+login_sql+';'
                                          'PWD='+password_sql+';'
                                          'Trusted_Connection=no;')
                    cursor = conn.cursor()
                    cursor.execute(
                        "declare @lot_id int; Select @lot_id = l.Lot_ID FROM [Cursor].[dbo].[Tender] t join [Cursor].[dbo].[Lot] l on t.Tender_ID=l.Tender_ID where t.NotifNr = '" + str(
                            notifnr) + "'; "
                                       "insert into [Cursor].[dbo].[LotSpec](Lot_ID, Pos, ProdType, ProdNm, Form, LS_MNN_ID, LS_TradeNm_ID, LotSpecMtdID, Price, Num, Summa, Unit_ID, SYSDATE, isAutomate, UserID) "
                                       "values (@lot_id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), '1', 'FDA506B0-2F4C-49F4-864F-836731C63391') ",
                        i_pos, prodtype, prodnm, form_pos, mnn_pos, tn_pos, mtd_pos, price_pos, amount_pos, sum_pos, ed_izm)
                    if isDebug == False:
                        conn.commit()
                    conn.close()

                    i_pos += 1
            else:
                    print('Нет позиций')

        print('Done')
        time.sleep(2)