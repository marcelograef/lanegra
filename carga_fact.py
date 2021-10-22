"""
    @author Juan Pablo Mantelli
"""
from db import main
import json
import requests
import pandas as pd
from datetime import datetime

import api_requests
import functions
from bd import get_connection, get_cursor

conexion = get_connection()
cursor = get_cursor()

if __name__ == "__main__":

    main()

    print()
    print("Starting carga int")
    print()

    id_lote = functions.getLoteKey('Carga_Int_Fact_Py')

    response_process = functions.getLastProcessKeyByLote(id_lote)
    functions.updateProcessState(response_process.Proceso_Key, 1)

    fechas = [
        response_process.fecha_desde_proceso,
        response_process.fecha_hasta_proceso
    ]

    print(fechas[0].strftime('%Y-%m-%d'))
    print(fechas[1].strftime('%Y-%m-%d'))

    print()
    print("get_date_range()")
    print(fechas)
    #fechas = functions.get_date_range()
    print(fechas)

    # Está aparte por que se envie una vez con dol y otra por pes, además del parametro tipo moneda en 1
    conDobleMoneda = [
        ['AnlisisisDeSanidad', "Int_Sanidad"],
        ['suplementacionHaciendaELN', "Int_Suplementacion"],
        ['analisisLiquidacionesGranos', "Int_Liq_Granos"],
        ['IngresosYEgresosDeHacienda', "Int_Hacienda_Ingresos_Egresos"],
        ['analisisLiquidacionesHacienda', "Int_Liquidaciones_Hacienda"]
    ]

    ingresosYEgresos = [
        ['ingresosYEgresos', "Int_Ingresos_Egresos_Deposito"]
    ]

    consumosProduccion = [
        ['analisisConsumosProduccion', "Int_Analisis_Consumo_Produccion"]
    ]

    planificaciones = [
        ['planificacionAgricola', "Int_Planificacion_Agricola"],
        ['planificacionForestal', "Int_Planificacion_Forestal"]
    ]

    mainstables2 = [
        ['AnalisisHaciendaEventoYClasificacion',
            "Int_Hacienda_Evento_Clasificacion"]
    ]

    analisislote = [
        # PARAMWEBREPORT_VerMonedaEn a este se le envia un 1 o un 2.. es distinto al resto
        ['AnalisisDeLote', "Int_Analisis_Lote"]
    ]

    stock = [
        ['existenciaHaciendaELN', "int_StockXLote"],
        ['existenciaHaciendaELN', "int_StockXLoteTropa"]
    ]

    libromayoreln = [
        ['LIBROMAYORELN', "Int_LIBROMAYORELN"]
    ]

    libromayorelnx = [
        ['LIBROMAYORELNACTIVO', "int_Mayor_Activo"],
        ['LIBROMAYORELNPASIVO', "Int_Mayor_Pasivo"],
        ['LIBROMAYORELNPN', "Int_Mayor_Pn"]
    ]

    response = api_requests.init()

    token = response.text

    for table in consumosProduccion:
        dfs = []
        monedas = ["PES", "DOL"]
        dimension = ["DIMCTC", "DIMMAQ"]

        for x in range(0, len(monedas)):
            for y in range(0, len(dimension)):

                myObj = []
                while True:
                    response = api_requests.int_conMoneda(
                        token, monedas[x], table[0], fechas, dimension[y])

                    myObj = response.json()

                    if api_requests.check_token(myObj) == True:
                        break

                    token = api_requests.init().text

                for field_dict in myObj:
                    field_dict['TIPO_DIMENSION'] = dimension[y]

                # functions.deleteTable(table[1])
                if len(myObj) > 0:
                    dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), table[1])
            # functions.auditoria(table[1], response_process.Proceso_Key)
    conexion.commit()

    for table in conDobleMoneda:
        dfs = []
        monedas = ["PES", "DOL"]
        # Agregue la dimesion moneda por que esta hardcodead un solo tipo DIMCTC y dim DIMMAQ
        dimension = ["DIMCTC", "DIMMAQ"]
        for x in range(0, len(monedas)):
            for y in range(0, len(dimension)):

                myObj = []
                while True:
                    response = api_requests.int_conMoneda(
                        token, monedas[x], table[0], fechas, dimension[y])  # remplaze 'DIMCTC' por dimension
                    print(response)
                    myObj = response.json()

                    if api_requests.check_token(myObj) == True:
                        break

                    token = api_requests.init().text

                if len(myObj) > 0:
                    dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), table[1])

    conexion.commit()

    for st in stock:
        dfs = []
        monedas = ["PES", "DOL"]
        agrupa_por = -1

        if st[1] == 'int_StockXLote':
            agrupa_por = 1

        if st[1] == 'int_StockXLoteTropa':
            agrupa_por = 0

        for x in range(0, len(monedas)):

            myObj = []
            while True:
                response = api_requests.int_stock(
                    token, monedas[x], st[0], fechas, agrupa_por)

                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break

                token = api_requests.init().text

            if len(myObj) > 0:
                dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), st[1])

    conexion.commit()
    for lme in libromayorelnx:
        dfs = []
        monedas = ["PES", "DOL"]

        for x in range(0, len(monedas)):

            myObj = []
            while True:
                print(datetime.now())
                response = api_requests.libromayorelnx(
                    token, monedas[x], fechas, lme[0])
                print(datetime.now())
                print(response)
                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break

                token = api_requests.init().text

            if len(myObj) > 0:
                dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), lme[1])

    conexion.commit()

    for lme in libromayoreln:
        dfs = []
        monedas = ["PES", "DOL"]
        dimension = ["DIMCTC", "DIMMAQ"]

        for x in range(0, len(monedas)):
            for y in range(0, len(dimension)):

                myObj = []
                while True:
                    response = api_requests.libromayoreln(
                        token, monedas[x], fechas,  dimension[y])
                    print(response)
                    myObj = response.json()

                    if api_requests.check_token(myObj) == True:
                        break

                    token = api_requests.init().text

                for field_dict in myObj:
                    field_dict['TIPO_DIMENSION'] = dimension[y]

                if len(myObj) > 0:
                    dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), lme[1])

    conexion.commit()
    for table2 in mainstables2:

        myObj = []
        while True:
            response = api_requests.interface(token, table2[0], fechas)

            myObj = response.json()

            if api_requests.check_token(myObj) == True:
                break

            token = api_requests.init().text

        # functions.deleteTable(table2[1])
        functions.insertDimension(pd.DataFrame(myObj), table2[1])
        # functions.auditoria(table2[1], response_process.Proceso_Key)

    conexion.commit()

    for planif in planificaciones:

        myObj = []
        while True:
            response = api_requests.planificaciones(token, planif[0])

            print(response)

            myObj = response.json()

            if api_requests.check_token(myObj) == True:
                break
            token = api_requests.init().text

        # functions.deleteTable(planif[1])
        functions.insertDimension(pd.DataFrame(myObj), planif[1])
        # functions.auditoria(planif[1], response_process.Proceso_Key)
    conexion.commit()

    for lote in analisislote:

        dfs = []

        monedas = [0, 1]

        for x in range(0, len(monedas)):

            myObj = []
            while True:
                response = api_requests.LoteAna(
                    token, monedas[x], lote[0], fechas)
                print(response)
                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break

                token = api_requests.init().text

            if len(myObj) > 0:
                dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), lote[1])

    conexion.commit()

    for table in ingresosYEgresos:

        dfs = []
        monedas = ["PES", "DOL"]

        for x in range(0, len(monedas)):
            myObj = []
            while True:
                response = api_requests.int_ingresosYEgresos(
                    token, monedas[x], table[0], fechas)
                print(response)
                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break

                token = api_requests.init().text

            if len(myObj) > 0:
                dfs.append(pd.DataFrame(myObj))

        if len(dfs) > 0:
            functions.insertDimension(pd.concat(dfs), table[1])

    # close the connection to the database.
    conexion.commit()
    functions.updateProcessState(response_process.Proceso_Key, 2)
    cursor.close()
