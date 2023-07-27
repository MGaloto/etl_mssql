import pyodbc 
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import pprint

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
        self.logsConnection = []
        self.logsDataBase = []
        self.date = self.getDateNow()

    def checkLenLogs(self,logs):
        if len(logs) > 1:
            return True
        else:
            return False
    
    def getDateNow(self):
        fecha_hora_actual = datetime.now()
        formato = "%Y-%m-%d %H:%M:%S.%f"
        date = fecha_hora_actual.strftime(formato)[:-3]
        return date
        
    def writeLogs(self, logs, type):
        new_date = self.date.replace(" ","").replace(":","").replace("-","")
        with open(f"logs/{new_date + type}.json", 'w') as archivo_json:
            json.dump(logs, archivo_json)
        

    def run(self):
        try:
            conn = pyodbc.connect(f"Driver={os.environ.get('MSSQL')};"
                          f"Server={os.environ.get('SERVER')};"
                          f"Database={os.environ.get('DB')};"
                          "Trusted_Connection=yes;")
            cursor = conn.cursor()
        except Exception  as e:
            print(f"Error: {e}")
            self.logsConnection.append({self.date:e})

        try:
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
        except Exception  as e:
            print(f"Error: {e}")
            self.logsDataBase.append({self.date + str(e)})

        cursor.commit()
        cursor.close()

        if self.checkLenLogs(self.logsConnection):
            self.writeLogs(self.logsConnection, "conn")
            pprint.pprint(self.logsConnection[0])
        if self.checkLenLogs(self.logsDataBase):
            self.writeLogs(self.logsDataBase, "insert")
            pprint.pprint(self.logsDataBase[0])


if __name__ == "__main__":
    table = "Unificado"
    objeto = ConfigDataBase(table)
    objeto.run()
    
    
    


    