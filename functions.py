"""
    @author Juan Pablo Mantelli
"""

from sqlalchemy import create_engine
import datetime
import numpy as np
import pandas as pd

from db import get_connection

import pyodbc

# create timer
start_time = time.time()


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def insertDimension(data, TABLE_NAME):

    print(datetime.datetime.now())
    print('Comenzando la carga de la tabla', TABLE_NAME)
    sqlstatement = ''
    sqlstatement2 = ''
    keylist2 = ''
    counter = 0
    # empty list
    my_list = []
    my_list_values = []

    # de esta manera el json tiene items para poder recorrerlos
    #data = [ r ]

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

    print(data)

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

    for col in data.columns:
        data[col] = data[col].apply(process_data)

    print(data)

    #print('Comenzando la carga de la tabla',TABLE_NAME)
    print(datetime.datetime.now())

    pd.to_sql(con=get_connection, index=False, if_exists='replace')

    for array in splits:
        my_list_str = listToString(array)
        my_list_str = my_list_str.replace("'", "''")
        sentence = "EXEC SP_Bulk_Insert '" + my_list_str + "'"
        carga_dim.cursor.execute(sentence)

    #my_list_str = listToString(first_half)
    #my_list_str = my_list_str.replace("'", "''")
    #sentence = "EXEC SP_Bulk_Insert '"+ my_list_str + "'"
    # carga_dim.cursor.execute(sentence)
#
    #my_list_str = listToString(second_half)
    #my_list_str = my_list_str.replace("'", "''")
    #sentence = "EXEC SP_Bulk_Insert '"+ my_list_str + "'"
    # carga_dim.cursor.execute(sentence)

   # for row_sql in my_list_str:
   #    #row_sql = listToString(row_sql)
   #    row_sql = row_sql.replace("'", "''")
   #    sentence = "EXEC SP_Bulk_Insert '"+ row_sql + "'"
   #    carga_dim.cursor.execute(sentence)
        # carga_dim.cursor.execute(row_sql)
        # carga_dim.cursor.commit()
    #my_list_str = my_list_str.replace('\'', "''")

    # print(my_list_str)
    #carga_dim.cursor.execute("EXEC SP_Bulk_Insert ("+ my_list_str + ")")
    # for row_sql in my_list:
        # print(row_sql)
        #row_sql = row_sql.replace("'", "''")
        #sentence = "EXEC SP_Bulk_Insert '"+ row_sql + "'"
        # carga_dim.cursor.execute(sentence)

        # carga_dim.cursor.execute(row_sql)

    print('Carga completa de la tabla', TABLE_NAME)
    print(datetime.datetime.now())


"""
def deleteTable(table):
    carga_dim.cursor.execute("DELETE FROM " + table)


def getLoteKey(descripcion):
    carga_dim.cursor.execute(
        "SELECT Lote_Key FROM Esq_Proc_Dw_Lotes WHERE Descripcion = '" + descripcion + "'")
    result = carga_dim.cursor.fetchone()
    for r in result:
        return r


def getLastProcessKeyByLote(idLote):
    carga_dim.cursor.execute(
        "SELECT MAX([Proceso_Key]) as Proceso_Key, fecha_desde_proceso, fecha_hasta_proceso FROM Esq_Proc_Dw_Procesos WHERE Lote_Key = (?) AND [Estado_Key] = 0 group by fecha_desde_proceso, fecha_hasta_proceso", (idLote))
    return carga_dim.cursor.fetchone()


def insertProcessKey(id_lote, fecha_inicio, fecha_fin):
    carga_dim.cursor.execute("INSERT INTO [dbo].[Esq_Proc_Dw_Procesos] ([Fecha_Inicio_Ejecucion],[Fecha_Desde_Proceso],[Fecha_Hasta_Proceso],[Estado_Key],[Lote_Key],[IdControl], IdUsuarioEjecucion) VALUES((?),'" +
                             fecha_inicio + "','" + fecha_fin + "',0,(?),0,3)", (datetime.datetime.now(), id_lote))
    carga_dim.cursor.commit()


def updateProcessState(process_key, state):
    carga_dim.cursor.execute(
        "UPDATE [dbo].[Esq_Proc_Dw_Procesos] SET [Estado_Key] = (?)  WHERE [Proceso_Key] = (?)", (state, process_key))
    carga_dim.cursor.commit()


def updateTable(table, field, value):
    carga_dim.cursor.execute("UPDATE " + table + " SET " +
                             field + " = (?) WHERE " + field + " IS NULL", (value))


def auditoria(table, process_key):
    carga_dim.cursor.execute(
        "UPDATE " + table + " SET fecha_proce_escritura = (?), Proceso_Key = (?)", (datetime.datetime.now(), process_key))

"""


def listToString(s):

    # initialize an empty string
    str1 = ""

    # traverse in the string
    for ele in s:
        str1 += ele

    # return string
    return str1


# def splitLists(list):
    ##length = len(list)
    # if length > 1000:
    ## total = length / 1000
#total = round(total)
    # middle_index = length // 2
    #first_half = list[:middle_index]
    #second_half = list[middle_index:]

    # print(first_half)
    # print(second_half)

# def chunkIt(seq, num):
    #avg = len(seq) / float(num)
    #out = []
    #last = 0.0

    # while last < len(seq):
    #    out.append(seq[int(last):int(last + avg)])
    #    last += avg

    # return out
