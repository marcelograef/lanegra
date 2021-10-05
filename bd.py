"""
    @author Juan Pablo Mantelli
"""
import pyodbc
from sqlalchemy import create_engine

server = 'SD-1887927-W\DEV'
database = 'DW_ELN'
username = 'SD-1887927-W\gestion'
password = 'LaNegra.1970'
driver = '{ODBC Driver 17 for SQL Server}'


def get_connection():
    return create_engine('mssql+pyodbc://' + server + '/' + database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')


if __name__ == '__main__':
    try:

        conexion = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes')

        cursor = conexion.cursor()
        cursor.execute("EXEC SP_Iniciar_ETL")
        print("\n"*2)
        print("Conexión exitosa a la base de datos")

    except Exception as e:
        print("Ocurrió un error al conectar a SQL Server: ", e)

    cursor.commit()
    cursor.close()
