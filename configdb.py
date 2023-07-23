import pandas as pd
import pyodbc 
import os
from dotenv import load_dotenv

load_dotenv()

class ConfigDataBase():
    """
    En esta clase se hace una instancia en la base de datos local MSSQL luego de restaurar la base de datos.
    En primero lugar se modifica la tabla quedandonos con el ultimo registro con FECHA_COPIA ultima por cada fila 
    duplicada en ID, MUESTRA, y RESULTADO.
    En segundo lugar se modifica la tabla para que solo se pueda hacer APPEND de una combinacion unica de [ID], [MUESTRA], [RESULTADO], [FECHA_COPIA] y no puedan insertarse duplicados.
    """
    def __init__(self, table):
        self.table = table
        

    def run(self):
        try:
            conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          f"Server={os.environ.get('SERVER')};"
                          f"Database={os.environ.get('DB')};"
                          "Trusted_Connection=yes;")
            cursor = conn.cursor()
        except Exception  as e:
            print(f"Error: {e}")

        total_registros_inical = cursor.execute(f"""SELECT COUNT(*) AS TotalRegistros
        FROM {self.table};""").fetchall()[0][0]
        print(f"Total registros inicial: {str(total_registros_inical)}")

        cursor.execute(f"""WITH CTE AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ID, MUESTRA, RESULTADO ORDER BY FECHA_COPIA DESC) AS rn FROM {self.table}) 
        DELETE FROM CTE WHERE rn > 1;""")

        cursor.execute(f"""ALTER TABLE {self.table}
                ADD CONSTRAINT UQ_UnicoValores UNIQUE ([ID], [MUESTRA], [RESULTADO], [FECHA_COPIA]);""")  
        total_registros_post = cursor.execute(f"""SELECT COUNT(*) AS TotalRegistros
        FROM {self.table};""").fetchall()[0][0]
        print(f"Total registros final: {str(total_registros_post)}")

        cursor.commit()
        cursor.close()


if __name__ == "__main__":
    table = "Unificado"
    objeto = ConfigDataBase(table)
    objeto.run()
    
    
    


    