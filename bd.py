"""
    @author Juan Pablo Mantelli
"""
import pyodbc

server = 'SD-1887927-W\DEV'
server = '179.43.118.246'
database = 'DW_ELN'
username = 'SD-1887927-W\gestion'
password = 'LaNegra.1970'
driver = '{ODBC Driver 17 for SQL Server}'

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
