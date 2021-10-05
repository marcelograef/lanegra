"""
    @author Juan Pablo Mantelli
"""
import time

import datetime
import numpy as np
import pandas as pd

from bd import get_engine, get_cursor


# create timer
# start_time = time.time()


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

def process_data(value):
    if type(value) in (str, bytes):
        value = value.replace("None", "null")
        value = str(value).replace("'", "")
    else:
        if value == True:
            value = 1
        elif value == False:
            value = 0
        value = str(value).replace("None", "null")
        value = str(value).replace("'", "")
    return value

def insertDimension(data, TABLE_NAME):

    start_time = time.time()
    
    print(f'{"-"*50}')
    print(f'{TABLE_NAME}')
    #print(datetime.datetime.now())
  
    columns_fixed = []
    for col in data.columns:
        col = col.replace("-", "")
        col = col.replace(" ", "")
        col = col.replace("@@", "")
        col = col.replace(" / ", "")
        col = col.replace("/", "")
        col = col.replace("%", "PORCENTAJE")
        col = col.replace("Ó", "o")
        col = col.replace(".", "_")
        col = col.replace("Í", "i")

        columns_fixed.append(col)

    data.columns = columns_fixed

    for col in data.columns:
        data[col] = data[col].apply(process_data)


    #print('Comenzando la carga de la tabla',TABLE_NAME)   
    data.to_sql(TABLE_NAME, con=get_engine(), index=False, if_exists='replace')


    print(f'{round(time.time() - start_time, 2)}seg')
    print()
    print()



def deleteTable(table):
    with get_cursor() as cursor:
        cursor.execute("DELETE FROM "+ table )

def getLoteKey(descripcion):
    with get_cursor() as cursor:
        cursor.execute("SELECT Lote_Key FROM Esq_Proc_Dw_Lotes WHERE Descripcion = '"+ descripcion +"'")
        result = cursor.fetchone()
        for r in result:
            return r

def getLastProcessKeyByLote(idLote):
    with get_cursor() as cursor:
        cursor.execute("SELECT MAX([Proceso_Key]) as Proceso_Key, fecha_desde_proceso, fecha_hasta_proceso FROM Esq_Proc_Dw_Procesos WHERE Lote_Key = (?) AND [Estado_Key] = 0 group by fecha_desde_proceso, fecha_hasta_proceso", (idLote))
        return cursor.fetchone()

def insertProcessKey(id_lote, fecha_inicio, fecha_fin):
    with get_cursor() as cursor:
        cursor.execute("INSERT INTO [dbo].[Esq_Proc_Dw_Procesos] ([Fecha_Inicio_Ejecucion],[Fecha_Desde_Proceso],[Fecha_Hasta_Proceso],[Estado_Key],[Lote_Key],[IdControl], IdUsuarioEjecucion) VALUES((?),'"+ fecha_inicio +"','"+ fecha_fin +"',0,(?),0,3)", (datetime.datetime.now(),id_lote))
        cursor.commit()

def updateProcessState(process_key, state):
    with get_cursor() as cursor:
        cursor.execute("UPDATE [dbo].[Esq_Proc_Dw_Procesos] SET [Estado_Key] = (?)  WHERE [Proceso_Key] = (?)", (state, process_key))
        cursor.commit()

def updateTable(table, field, value):
    with get_cursor() as cursor:
        cursor.execute("UPDATE "+ table +" SET "+ field +" = (?) WHERE "+ field +" IS NULL", (value))


def auditoria(table, process_key):
    print()
    print("AUDITORIA")
    print(table, process_key)
    with get_cursor() as cursor:
        cursor.execute("UPDATE "+ table +" SET fecha_proce_escritura = (?), Proceso_Key = (?)", (datetime.datetime.now(),process_key))
