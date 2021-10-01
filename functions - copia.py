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

        
        my_list.append(keylist)
        my_list_values.append(valuelist)
        my_list_values.append(',')

        counter = counter + 1

    print(counter // 999)

    #B, C = split_list(my_list_values)

    my_list = list(dict.fromkeys(my_list))
    

    lToStringColumns = listToString(my_list)
    lToStringValues = listToString(my_list_values)

    ##Le saco la última coma que me quede por appendearla más arriba.
    lToStringValues = lToStringValues[:-1]
       
    sqlstatement = "INSERT INTO " + TABLE_NAME + " " + lToStringColumns + " VALUES " + lToStringValues + "\n"

    #cursor.execute("insert into products(id, name) values (?, ?)", 'pyodbc', 'awesome library')
    #cur.executemany(query, values)
    #cnxn.commit()

    #sqlstatement = "EXEC SP_Insert_Bulk " + TABLE_NAME + "," + lToStringColumns + "," + lToStringValues + " "


    carga_dim.cursor.execute(sqlstatement)

    print('Carga completa de la tabla',TABLE_NAME)



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


def listToString(s): 
    
    # initialize an empty string
    str1 = "" 
    
    # traverse in the string  
    for ele in s: 
        str1 += ele  
    
    # return string  
    return str1 

def split_list(a_list):
    half = len(a_list)//2
    return a_list[:half], a_list[half:]