"""
    @author Juan Pablo Mantelli
"""
import json
import requests
import pandas as pd
from datetime import datetime
import traceback

import datetime

import api_requests
import functions

from bd import main
from bd import get_connection, get_cursor

withDB = True

if withDB:
    conexion = get_connection()
    cursor = get_cursor()

if __name__ == "__main__":
    if withDB:
        main()

    print()
    print("Starting carga int")
    print()

    if withDB:
        id_lote = functions.getLoteKey('Carga_Int_Fact_Py')

        response_process = functions.getLastProcessKeyByLote(id_lote)
        functions.updateProcessState(response_process.Proceso_Key, 1)

    try:
        fechas = [
            response_process.fecha_desde_proceso,
            response_process.fecha_hasta_proceso
        ]
    except:
        fechas = [datetime.datetime(2021, 1, 1, 0, 0),
                  datetime.datetime(2021, 1, 31, 0, 0)]

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

    total_tables = {'tables': len(consumosProduccion), 'fail': 0}

    print()
    print(f"{'*'*50}")
    print('consumosProduccion')
    for table in consumosProduccion:
        try:
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

                    """ for field_dict in myObj:
                        print(myObj)
                        print(dimension, dimension[y])

                        field_dict['TIPO_DIMENSION'] = dimension[y] """

                    # functions.deleteTable(table[1])

                    print(f"Config {monedas[x]} {dimension[y]}")
                    # print(myObj)
                    if len(myObj) > 0:
                        df = pd.DataFrame(myObj)

                        df['TIPO_DIMENSION'] = dimension[y]

                        print(
                            f"Shape: {df.shape}")
                        dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")
                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), table[1], response_process.Proceso_Key)
                    # functions.auditoria(table[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', table[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(conDobleMoneda)

    print('conDobleMoneda')
    for table in conDobleMoneda:
        myObj = None
        try:
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
                        myObj = response.json()

                        if api_requests.check_token(myObj) == True:
                            break

                        token = api_requests.init().text

                    print(f"Config {monedas[x]} {dimension[y]}")
                    # print(myObj)
                    if len(myObj) > 0:
                        df = pd.DataFrame(myObj)

                        print(
                            f"Shape: {df.shape}")
                        dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")
                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), table[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            print()
            print(myObj)
            print()
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', table[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(stock)

    print('stock')
    for st in stock:
        myObj = None
        try:
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
                    df = pd.DataFrame(myObj)

                    print(
                        f"Shape: {df.shape}")
                    dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")
                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), st[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', st[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(libromayorelnx)

    print('libromayorelnx')
    for lme in libromayorelnx:
        try:
            dfs = []
            monedas = ["PES", "DOL"]

            for x in range(0, len(monedas)):

                myObj = []
                while True:
                    response = api_requests.libromayorelnx(
                        token, monedas[x], fechas, lme[0])
                    myObj = response.json()

                    if api_requests.check_token(myObj) == True:
                        break

                    token = api_requests.init().text

                if len(myObj) > 0:
                    df = pd.DataFrame(myObj)

                    print(
                        f"Shape: {df.shape}")
                    dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")
                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), lme[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', lme[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(libromayoreln)

    print('libromayoreln')
    for lme in libromayoreln:
        try:
            dfs = []
            monedas = ["PES", "DOL"]
            dimension = ["DIMCTC", "DIMMAQ"]

            for x in range(0, len(monedas)):
                for y in range(0, len(dimension)):

                    myObj = []
                    while True:
                        response = api_requests.libromayoreln(
                            token, monedas[x], fechas,  dimension[y])
                        myObj = response.json()

                        if api_requests.check_token(myObj) == True:
                            break

                        token = api_requests.init().text

                    """ for field_dict in myObj:
                        field_dict['TIPO_DIMENSION'] = dimension[y] """

                    if len(myObj) > 0:
                        df = pd.DataFrame(myObj)
                        df['TIPO_DIMENSION'] = dimension[y]

                        print(
                            f"Shape: {df.shape}")
                        dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")

                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), lme[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', lme[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(mainstables2)

    print('mainstables2')
    for table2 in mainstables2:

        try:
            myObj = []
            while True:
                response = api_requests.interface(token, table2[0], fechas)

                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break

                token = api_requests.init().text

            df = pd.DataFrame(myObj)
            print(f"Shape TOTAL {df.shape}")

            if withDB:
                # functions.deleteTable(table2[1])
                functions.insertDimension(
                    df, table2[1], response_process.Proceso_Key)
            # functions.auditoria(table2[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', table2[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(planificaciones)

    print('planificaciones')
    for planif in planificaciones:
        try:
            myObj = []
            while True:
                response = api_requests.planificaciones(token, planif[0])
                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break
                token = api_requests.init().text

            df = pd.DataFrame(myObj)
            print(f"Shape TOTAL {df.shape}")

            if withDB:
                # functions.deleteTable(planif[1])
                functions.insertDimension(
                    df, planif[1], response_process.Proceso_Key)
            # functions.auditoria(planif[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            functions.insertInconsistencia(
                response_process.Proceso_Key, 'No se pudieron cargar los datos', planif[1])
            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(analisislote)

    print('analisislote')
    for lote in analisislote:
        try:
            dfs = []

            monedas = [0, 1]

            for x in range(0, len(monedas)):

                myObj = []
                while True:
                    response = api_requests.LoteAna(
                        token, monedas[x], lote[0], fechas)
                    myObj = response.json()

                    if api_requests.check_token(myObj) == True:
                        break

                    token = api_requests.init().text

                if len(myObj) > 0:
                    df = pd.DataFrame(myObj)

                    print(
                        f"Shape: {df.shape}")
                    dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")

                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), lote[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', lote[1])

            total_tables['fail'] = total_tables['fail'] + 1

    if withDB:
        conexion.commit()

    print()
    print(f"{'*'*50}")

    total_tables['tables'] = total_tables['tables'] + len(ingresosYEgresos)

    print('ingresosYEgresos')
    for table in ingresosYEgresos:
        try:
            dfs = []
            monedas = ["PES", "DOL"]

            for x in range(0, len(monedas)):
                myObj = []
                while True:
                    response = api_requests.int_ingresosYEgresos(
                        token, monedas[x], table[0], fechas)
                    myObj = response.json()

                    if api_requests.check_token(myObj) == True:
                        break

                    token = api_requests.init().text

                if len(myObj) > 0:
                    df = pd.DataFrame(myObj)

                    print(
                        f"Shape: {df.shape}")
                    dfs.append(df)

            if len(dfs) > 0:
                print(f"Shape TOTAL {pd.concat(dfs).shape}")

                if withDB:
                    functions.insertDimension(
                        pd.concat(dfs), table[1], response_process.Proceso_Key)
        except:
            print(traceback.print_exc())
            if withDB:
                functions.insertInconsistencia(
                    response_process.Proceso_Key, 'No se pudieron cargar los datos', table[1])

            total_tables['fail'] = total_tables['fail'] + 1

    print()
    print(f"{'*'*50}")
    print("Guardando estado fina")
    # close the connection to the database.

    if withDB:
        conexion.commit()

    if total_tables['tables'] == total_tables['fail']:
        if withDB:
            functions.updateProcessState(response_process.Proceso_Key, 4)
    elif total_tables['fail'] > 0:
        if withDB:
            functions.updateProcessState(response_process.Proceso_Key, 3)
    else:
        if withDB:
            functions.updateProcessState(response_process.Proceso_Key, 2)

    if withDB:
        cursor.close()

    print()
    print(f"{'*'*50}")
    print("Carga finalizada")
    print(f"{'*'*50}")
