"""
    @author Juan Pablo Mantelli
"""
import json
import requests
import pandas as pd

import api_requests
import functions
from bd import conexion

cursor = conexion.cursor()

if __name__ == "__main__":

    id_lote = functions.getLoteKey('Carga_Int_Dim_Py')
    response_process = functions.getLastProcessKeyByLote(id_lote)
    functions.updateProcessState(response_process.Proceso_Key, 1)

    dimensions = [
        ['empresa',"Int_Dim_Empresa"],
        ['cuenta',"Int_Dim_Cuenta"],
        ['zona',"Int_Dim_Zona"],
        ['haciendacategoria',"Int_Dim_Hacienda_Categoria"],
        ['establecimiento',"Int_Dim_Establecimiento"],
        ['producto',"Int_Dim_Producto"],
        ['lote',"Int_Dim_Lote"],
        ['CentroCostoBSA',"Int_Dim_Centro_Costo"],
        ['moneda',"Int_Dim_Moneda"],
        ['unidad',"Int_Dim_Unidad"],
        ['cliente',"Int_Dim_Cliente"],
        ['campana',"Int_Dim_Campana"],
        ['condicionPago',"Int_Dim_Condicion_Pago"],
        ['conceptoValorizacion',"Int_Dim_Concepto_Valorizacion"],
        ['Organizacion',"Int_Dim_Organizacion"],
        ['labor',"Int_Dim_Labor"],
       #['actividad',"Int_Dim_Actividad"]
    ]

    dimensions_w_parameters = [
        ['arbolCategoriaHacienda',"Int_Dim_Categoria_Hacienda_Arbol"],
        ['arbolCategoriaHacienda',"Int_Dim_Categoria_Producto_Arbol"],
        ['arbolCentroCosto',"Int_Dim_Centro_Costo_Arbol"],
        ['arbolLabor',"Int_Dim_Labor_Arbol"]
    ]


    parameters = [
        ########################################################### deje cargada la de arbolcentrocosto!!!!
        ['arbolCentroCosto',"Arbol TAG Centro de Costo50213"],
        ['arbolCentroCosto',"CC COM AGR POR CULTIVO50203"],
        ['arbolCentroCosto',"CC Cuadro de Resultado ELN50254"],
        ['arbolCentroCosto',"CC Cuadro de Resultado ELN (CAMPAÑA)50255"],
        ['arbolCentroCosto',"CC ESTRUCTURA50221"],
        ['arbolCentroCosto',"CC GANADEROS POR ESTABLECIMIENTOS50202"],
        ['arbolCentroCosto',"CC Indirectos por Establec.50218"],
        ['arbolCentroCosto',"CC Indirectos, ADM y Gs Financ.50205"],
        ['arbolCentroCosto',"CC POR ESTABLECIMIENTO50198"],
        ['arbolCentroCosto',"CC Prueba50252"],
        ['arbolCentroCosto',"CENTROS DE COSTO_STD"],
        ['arbolCentroCosto',"Centros de Costo (bis)50251"],
        ['arbolCentroCosto',"CLASIFICACION CUADRO RESULTADO50225"],
        ['arbolCentroCosto',"DIRECTORIO - AGRICULTURA50227"],
        ['arbolCentroCosto',"INFORME GESTIÓN AGRÍCOLA - ELN50191"],
        ['arbolCentroCosto',"INFORME GESTIÓN GANADERA - ELN50199"],
        ['arbolCentroCosto',"INFORME GESTIÓN GANADERA - ELN POR ACTIVIDAD50200"],
        ['arbolCentroCosto',"Jurisdicciones50171"],
        ['arbolCentroCosto',"TAG - MB La Asunción50219"],
        ['Int_Dim_Categoria_Hacienda_Arbol',"HACIENDACATEGORIA_STD"],
        ['Int_Dim_Categoria_Producto_Arbol',"PRODUCTOS_STD"],
        ['arbolLabor',"LABOR_STD"]
    ]


    print(id_lote)
    print(response_process)


    try:
        for dim in dimensions:

            response = api_requests.init()

            token = response.text

            response = api_requests.dimension(token, dim[0])

            myObj = response.json()


            functions.deleteTable(dim[1])
            functions.insertDimension(myObj, dim[1])
            functions.auditoria(dim[1], response_process.Proceso_Key)

    except Exception as e_dim:
        print("Ocurrió un error al cargar dimensiones ", e_dim)
        conexion.commit()


    try:
        for dimw in dimensions_w_parameters:

            functions.deleteTable(dimw[1])

            for rama in parameters:

                if ( rama[0] ==  dimw[1] ):
                    response = api_requests.dimensions_w_parameters(token, dimw[0], rama[1])

                    myObj = response.json()

                    functions.insertDimension(myObj, dimw[1])

        functions.auditoria(dimw[1], response_process.Proceso_Key)
    #close the connection to the database.

    except Exception as e_dimw:
            print("Ocurrió un error al cargar dimensiones con parametros ", e_dimw)

            conexion.commit()

            functions.auditoria(dimw[1], response_process.Proceso_Key)
            functions.updateProcessState(response_process.Proceso_Key, 2)
            conexion.commit()
            cursor.close()
