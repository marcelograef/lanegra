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

if __name__ == "__main__":

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
        ['actividad', "Int_Dim_Actividad"] # este endpoint no está implementado
    ]

    dimensions_w_parameters = [
        ['arbolCategoriaHacienda', "Int_Dim_Categoria_Hacienda_Arbol"],
        ['arbolCategoriaHacienda', "Int_Dim_Categoria_Producto_Arbol"],
        ['arbolCentroCosto', "Int_Dim_Centro_Costo_Arbol"],
        ['arbolLabor', "Int_Dim_Labor_Arbol"]
    ]

    parameters = [
        # deje cargada la de arbolcentrocosto!!!!
        ['arbolCentroCosto', "Arbol TAG Centro de Costo50213"],
        ['arbolCentroCosto', "CC COM AGR POR CULTIVO50203"],
        ['arbolCentroCosto', "CC Cuadro de Resultado ELN50254"],
        ['arbolCentroCosto', "CC Cuadro de Resultado ELN (CAMPAÑA)50255"],
        ['arbolCentroCosto', "CC ESTRUCTURA50221"],
        ['arbolCentroCosto', "CC GANADEROS POR ESTABLECIMIENTOS50202"],
        ['arbolCentroCosto', "CC Indirectos por Establec.50218"],
        ['arbolCentroCosto', "CC Indirectos, ADM y Gs Financ.50205"],
        ['arbolCentroCosto', "CC POR ESTABLECIMIENTO50198"],
        ['arbolCentroCosto', "CC Prueba50252"],
        ['arbolCentroCosto', "CENTROS DE COSTO_STD"],
        ['arbolCentroCosto', "Centros de Costo (bis)50251"],
        ['arbolCentroCosto', "CLASIFICACION CUADRO RESULTADO50225"],
        ['arbolCentroCosto', "DIRECTORIO - AGRICULTURA50227"],
        ['arbolCentroCosto', "INFORME GESTIÓN AGRÍCOLA - ELN50191"],
        ['arbolCentroCosto', "INFORME GESTIÓN GANADERA - ELN50199"],
        ['arbolCentroCosto', "INFORME GESTIÓN GANADERA - ELN POR ACTIVIDAD50200"],
        ['arbolCentroCosto', "Jurisdicciones50171"],
        ['arbolCentroCosto', "TAG - MB La Asunción50219"],
        ['Int_Dim_Categoria_Hacienda_Arbol', "HACIENDACATEGORIA_STD"],
        ['Int_Dim_Categoria_Producto_Arbol', "PRODUCTOS_STD"],
        ['arbolLabor', "LABOR_STD"]
    ]

    process_errors = []

    response = api_requests.init()

    token = response.text

    print(token)

    try:
        for dim in dimensions:

            response = api_requests.dimension(token, dim[0])

            myObj = response.json()

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
                    response = api_requests.dimensions_w_parameters(
                        token, dimw[0], rama[1])

                    myObj = response.json()

                    functions.insertDimension(pd.DataFrame(myObj), dimw[1])

    # close the connection to the database.

    except Exception as e_dimw:
        #print("Ocurrió un error al cargar dimensiones con parametros ", e_dimw)
        process_errors.append(dimw)

    if len(process_errors) >0:
        print("*******************")
        print("Tablas que no cargaron")
        for e in process_errors:
            print(e)

    print()
    print(f'{"*"*50}')
    print()
    print(f"Procesamiento total en {round(time.time() - start_time, 2)} segundos")
    print()
    print(f'{"*"*50}')


