"""
    @author Juan Pablo Mantelli
"""
import time

import datetime
import numpy as np
import pandas as pd

from bd import get_connection


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
    data.to_sql(TABLE_NAME, con=get_connection(), index=False, if_exists='replace')


    print(f'{round(time.time() - start_time, 2)}seg')
    print()
    print()


