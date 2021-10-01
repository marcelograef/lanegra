"""
    @author Juan Pablo Mantelli 
"""

import carga_dim
import datetime


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def insertDimension(r, d):

    TABLE_NAME = d
    print(datetime.datetime.now())
    print('Comenzando la carga de la tabla',TABLE_NAME)
    sqlstatement = ''
    sqlstatement2 = ''
    keylist2 = ''
    counter = 0
    # empty list
    my_list = []
    my_list_values = []

    ## de esta manera el json tiene items para poder recorrerlos
    #data = [ r ]
    data = r 
    
    for json in data:
        keylist = "("
        valuelist = "("
        firstPair = True
        for key, value in json.items():
            if not firstPair:
                keylist += ", "
                valuelist += ", "
            firstPair = False
            ## ALGUNOS CAMPOS VIENEN CON GUIONES O ESPACIOS, ACA LIMPIO ESO
            key = key.replace("-", "")
            key = key.replace(" ", "")
            key = key.replace("@@", "")
            key = key.replace(" / ", "")
            key = key.replace("/", "")
            key = key.replace("%", "PORCENTAJE")
            key = key.replace("Ó", "o")
            key = key.replace(".", "_")
            key = key.replace("Í", "i")
            keylist += key
            if type(value) in (str, bytes):
                value = value.replace("None", "null")
                value = str(value).replace("'", "")
                valuelist += "'" + value + "'"
            else:
                if value==True:
                    value = 1
                elif value==False:
                    value = 0
                value = str(value).replace("None", "null")
                value = str(value).replace("'", "")
                valuelist += str(value)
        keylist += ")"
        valuelist += ")"

        
        sqlstatement += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "\n"
        my_list.append("INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist)
    
    #print('Comenzando la carga de la tabla',TABLE_NAME)
    print(datetime.datetime.now())
    #print(my_list)
    for row_sql in my_list:
        #print(row_sql)
        carga_dim.cursor.execute(row_sql)

    print('Carga completa de la tabla',TABLE_NAME)
    print(datetime.datetime.now())



def deleteTable(table):
    carga_dim.cursor.execute("DELETE FROM "+ table )

def auditoria(table, process_key):
    carga_dim.cursor.execute("UPDATE "+ table +" SET fecha_proce_escritura = (?), Proceso_Key = (?)", (datetime.datetime.now(),process_key))

def getLoteKey(descripcion):
    carga_dim.cursor.execute("SELECT Lote_Key FROM Esq_Proc_Dw_Lotes WHERE Descripcion = '"+ descripcion +"'")
    result = carga_dim.cursor.fetchone()
    for r in result:
        return r
    
def getLastProcessKeyByLote(idLote):
    carga_dim.cursor.execute("SELECT MAX([Proceso_Key]) as Proceso_Key, fecha_desde_proceso, fecha_hasta_proceso FROM Esq_Proc_Dw_Procesos WHERE Lote_Key = (?) AND [Estado_Key] = 0 group by fecha_desde_proceso, fecha_hasta_proceso", (idLote))
    return carga_dim.cursor.fetchone()
    
def insertProcessKey(id_lote, fecha_inicio, fecha_fin):
    carga_dim.cursor.execute("INSERT INTO [dbo].[Esq_Proc_Dw_Procesos] ([Fecha_Inicio_Ejecucion],[Fecha_Desde_Proceso],[Fecha_Hasta_Proceso],[Estado_Key],[Lote_Key],[IdControl], IdUsuarioEjecucion) VALUES((?),'"+ fecha_inicio +"','"+ fecha_fin +"',0,(?),0,3)", (datetime.datetime.now(),id_lote))
    carga_dim.cursor.commit()

def updateProcessState(process_key, state):
    carga_dim.cursor.execute("UPDATE [dbo].[Esq_Proc_Dw_Procesos] SET [Estado_Key] = (?)  WHERE Proceso_Key = (?)", (state, process_key))
    carga_dim.cursor.commit()

def updateTable(table, field, value):
    carga_dim.cursor.execute("UPDATE "+ table +" SET "+ field +" = (?) WHERE "+ field +" IS NULL", (value))
