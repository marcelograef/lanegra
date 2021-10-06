"""
    @author Juan Pablo Mantelli
"""
import requests


def init():

    parameters = {
        "grant_type": 'client_credentials',
        "client_id": '0295d12cd456322141e7ea1ae4e6f07e',
        "client_secret": '5325232495636ab6fdbbd24e72ceeccd'
    }

    return requests.get("https://api.teamplace.finneg.com/api/oauth/token?", params=parameters)


def check_token(myObj):
    if 'error' in myObj.keys():
        if myObj['error'] == 'invalid token':
            return False
    return True


def libromayoreln(token, moneda, fechas, dimension):  # = remplaze'DIMCTC' por dimension
    print(dimension)
    print(moneda)
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_dimension": dimension,
        "PARAMWEBREPORT_verpor": '0',
        "PARAMWEBREPORT_Moneda": moneda,
        "PARAMWEBREPORT_Empresa": 'EMPRE01'  # Validar
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/libromayoreln?", params=parameters)


def libromayorelnx(token, moneda, fechas, libromayor):
    print(moneda)
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_Moneda": moneda,
        "PARAMWEBREPORT_Empresa": 'EMPRE01'
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+libromayor+"?", params=parameters)


def interface(token, interface, fechas):

    parameters = {
        "ACCESS_TOKEN": token,
        # remplaze '2021-01-01' desde principio mes
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        # remplaze '2021-01-31' hasta current day
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d')
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+interface+"?", params=parameters)


def sanidad(token, moneda, fechas):

    parameters = {
        "ACCESS_TOKEN": token,
        # remplaze '2021-01-01'
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        # remplaze '2021-01-31'
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_TipoPrecio": 1,
        "PARAMWEBREPORT_Moneda": moneda
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/AnlisisisDeSanidad?", params=parameters)


def suplementacion(token, moneda, fechas):

    parameters = {
        "ACCESS_TOKEN": token,
        # remplaze '2021-01-01'
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        # remplaze '2021-01-31'
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_TipoPrecio": 1,
        "PARAMWEBREPORT_Moneda": moneda
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/suplementacionHaciendaELN?", params=parameters)


# = remplaze'DIMCTC' por dimension
def int_conMoneda(token, moneda, reporte, fechas, dimension):
    print(dimension)
    print(moneda)
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_TipoPrecio": 1,
        "PARAMWEBREPORT_Moneda": moneda,
        "PARAMWEBREPORT_dimension": dimension
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+reporte+"?", params=parameters)


def int_stock(token, moneda, reporte, fechas, agrupa_por):
    print(moneda)
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_AgruparPor": agrupa_por,
        "PARAMWEBREPORT_Moneda": moneda,
        "PARAMWEBREPORT_tipoStock": 0,
        "PARAMWEBREPORT_soloStockNoCero": 1,
        "PARAMWEBREPORT_TipoPrecio": 1,
        "PARAMWEBREPORT_Turno": 0
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+reporte+"?", params=parameters)


def planificaciones(token, reporte):
    # no tiene fecha como parametro
    empresa = ''
    if reporte == 'planificacionAgricola':
        empresa = '[EMPRE01,ESC47]'
    elif reporte == 'planificacionForestal':
        empresa = '[CYD43]'
    print(empresa)
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_Empresa": empresa,
        "PARAM_SITUACIONPLANES": '1'
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+reporte+"?", params=parameters)


def dimension(token, dimension):

    parameters = {
        "ACCESS_TOKEN": token
    }

    return requests.get("https://api.teamplace.finneg.com/api/"+dimension+"/list?", params=parameters)


def dimensions_w_parameters(token, dimension, p):

    parameters = {
        "ACCESS_TOKEN": token,
        "Arbol": p,
        "Detalle": 0
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+dimension+"?", params=parameters)


def LoteAna(token, moneda, reporte, fechas):
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_costoStandard": 1,
        "PARAMWEBREPORT_incluirlabores": 3,
        "PARAMWEBREPORT_Ordenado": 'false',
        "PARAMWEBREPORT_Ejecutado": 'true',
        "PARAMWEBREPORT_Planificado": 'false',
        "PARAMWEBREPORT_VerMonedaEn": moneda
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+reporte+"?", params=parameters)


def int_ingresosYEgresos(token, moneda, reporte, fechas):
    print(moneda)
    parameters = {
        "ACCESS_TOKEN": token,
        "PARAMWEBREPORT_fechaDesde": fechas[0].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_fechaHasta": fechas[1].strftime('%Y-%m-%d'),
        "PARAMWEBREPORT_tipoStock": 0,
        "PARAMWEBREPORT_Empresa": "EMPRE01",
        "PARAMWEBREPORT_Moneda": moneda
        # "PARAMWEBREPORT_TipoPrecio": 1,
    }

    return requests.get("https://api.teamplace.finneg.com/api/reports/"+reporte+"?", params=parameters)
