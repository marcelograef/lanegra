"""
    @author Juan Pablo Mantelli
"""
import json

import pandas as pd
import requests
import traceback
import time

import api_requests
import functions

from db import main

if __name__ == "__main__":

    main()

    id_lote = functions.getLoteKey('Carga_Int_Dim_Py')
    response_process = functions.getLastProcessKeyByLote(id_lote)
    functions.updateProcessState(response_process.Proceso_Key, 1)

    start_time = time.time()
    dimensions = [
        ['empresa', "Int_Dim_Empresa"],
        ['cuenta', "Int_Dim_Cuenta"],
        ['zona', "Int_Dim_Zona"],
        ['haciendacategoria', "Int_Dim_Hacienda_Categoria"],
        ['establecimiento', "Int_Dim_Establecimiento"],
        ['producto', "Int_Dim_Producto"],
        ['lote', "Int_Dim_Lote"],
        ['CentroCostoBSA', "Int_Dim_Centro_Costo"],
        ['moneda', "Int_Dim_Moneda"],
        ['unidad', "Int_Dim_Unidad"],
        ['cliente', "Int_Dim_Cliente"],
        ['campana', "Int_Dim_Campana"],
        ['condicionPago', "Int_Dim_Condicion_Pago"],
        ['conceptoValorizacion', "Int_Dim_Concepto_Valorizacion"],
        ['Organizacion', "Int_Dim_Organizacion"],
        ['labor', "Int_Dim_Labor"],
        # este endpoint no está implementado
        ['actividad', "Int_Dim_Actividad"]
    ]

    dimensions_w_parameters = [
        ['arbolCategoriaHacienda', "Int_Dim_Categoria_Hacienda_Arbol"],
        ['arbolCategoriaHacienda', "Int_Dim_Categoria_Producto_Arbol"],
        ['arbolCentroCosto', "Int_Dim_Centro_Costo_Arbol"],
        ['arbolLabor', "Int_Dim_Labor_Arbol"]
    ]

    parameters = [
        # deje cargada la de arbolcentrocosto!!!!
        ['Int_Dim_Centro_Costo_Arbol', "Arbol TAG Centro de Costo50213"],
        ['Int_Dim_Centro_Costo_Arbol', "CC COM AGR POR CULTIVO50203"],
        ['Int_Dim_Centro_Costo_Arbol', "CC Cuadro de Resultado ELN50254"],
        ['Int_Dim_Centro_Costo_Arbol',
            "CC Cuadro de Resultado ELN (CAMPAÑA)50255"],
        ['Int_Dim_Centro_Costo_Arbol', "CC ESTRUCTURA50221"],
        ['Int_Dim_Centro_Costo_Arbol', "CC GANADEROS POR ESTABLECIMIENTOS50202"],
        ['Int_Dim_Centro_Costo_Arbol', "CC Indirectos por Establec.50218"],
        ['Int_Dim_Centro_Costo_Arbol', "CC Indirectos, ADM y Gs Financ.50205"],
        ['Int_Dim_Centro_Costo_Arbol', "CC POR ESTABLECIMIENTO50198"],
        ['Int_Dim_Centro_Costo_Arbol', "CC Prueba50252"],
        ['Int_Dim_Centro_Costo_Arbol', "CENTROS DE COSTO_STD"],
        ['Int_Dim_Centro_Costo_Arbol', "Centros de Costo (bis)50251"],
        ['Int_Dim_Centro_Costo_Arbol', "CLASIFICACION CUADRO RESULTADO50225"],
        ['Int_Dim_Centro_Costo_Arbol', "DIRECTORIO - AGRICULTURA50227"],
        ['Int_Dim_Centro_Costo_Arbol', "INFORME GESTIÓN AGRÍCOLA - ELN50191"],
        ['Int_Dim_Centro_Costo_Arbol', "INFORME GESTIÓN GANADERA - ELN50199"],
        ['Int_Dim_Centro_Costo_Arbol',
            "INFORME GESTIÓN GANADERA - ELN POR ACTIVIDAD50200"],
        ['Int_Dim_Centro_Costo_Arbol', "Jurisdicciones50171"],
        ['Int_Dim_Centro_Costo_Arbol', "TAG - MB La Asunción50219"],
        ['Int_Dim_Categoria_Hacienda_Arbol', "HACIENDACATEGORIA_STD"],
        ['Int_Dim_Categoria_Producto_Arbol', "PRODUCTOS_STD"],
        ['Int_Dim_Labor_Arbol', "LABOR_STD"]
    ]

    process_errors = []

    token = api_requests.init().text

    print(token)

    try:
        for dim in dimensions:
            myObj = []
            while True:
                response = api_requests.dimension(token, dim[0])
                myObj = response.json()

                if api_requests.check_token(myObj) == True:
                    break

                token = api_requests.init().text

            df = pd.DataFrame(myObj)

            functions.insertDimension(df, dim[1])

    except Exception as e_dim:
        # traceback.print_exc()
        # print("Ocurrió un error al cargar dimensiones ", e_dim)
        process_errors.append(dim)

    try:
        for dimw in dimensions_w_parameters:
            for rama in parameters:
                if (rama[0] == dimw[1]):
                    myObj = []
                    while True:
                        response = api_requests.dimensions_w_parameters(
                            token, dimw[0], rama[1])

                        myObj = response.json()

                        if api_requests.check_token(myObj) == True:
                            break

                        token = api_requests.init().text

                    functions.insertDimension(pd.DataFrame(myObj), dimw[1])

    # close the connection to the database.

    except Exception as e_dimw:
        #print("Ocurrió un error al cargar dimensiones con parametros ", e_dimw)
        process_errors.append(dimw)

    if len(process_errors) == 0:
        print("*******************")
        print("Tablas que no cargaron")
        for e in process_errors:
            print(e)
        functions.updateProcessState(response_process.Proceso_Key, 2)
    else:
        functions.updateProcessState(response_process.Proceso_Key, 3)

    # si falla todo
    # functions.updateProcessState(response_process.Proceso_Key, 4)

    print()
    print(f'{"*"*50}')
    print()
    print(
        f"Procesamiento total en {round(time.time() - start_time, 2)} segundos")
    print()
    print(f'{"*"*50}')
