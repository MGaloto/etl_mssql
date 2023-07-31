import pandas as pd
import pyodbc 
import os
from dotenv import load_dotenv
from datetime import datetime
import pprint
import json
import time

load_dotenv()

class ETL():
    """
    En esta clase se insertan los datos del csv en mssql la tabla Unificado.
    """
    def __init__(self, table):
        self.table = table
        self.logsConnection = []
        self.logsDataBase = []
        self.date = self.getDateNow()
        
    def getData(self):
        data = pd.read_csv(os.environ.get('URL'))
        return data
    
    def getDateNow(self):
        fecha_hora_actual = datetime.now()
        formato = "%Y-%m-%d %H:%M:%S.%f"
        date = fecha_hora_actual.strftime(formato)[:-3]
        return date
    
    def time_func(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            func(*args,**kwargs)
            end = time.time()
            print(f'Elapsed time: {(end - start)*1000:.3f}ms')
        return wrapper
    
    @time_func
    def insert_data(self, data, query, cursor):
        count = 0
        for valor in data:
            count = count + 1
            try:
                cursor.executemany(query, [valor])
            except Exception as e:
                self.logsDataBase.append({self.date + str(count):str(e)})
        return

    @time_func
    def writeLogs(self, logs, type):
        new_date = self.date.replace(" ","").replace(":","").replace("-","")
        with open(f"logs/{new_date + type}.json", 'w') as archivo_json:
            json.dump(logs, archivo_json)
    
    def checkLenLogs(self,logs):
        if len(logs) > 1:
            return True
        else:
            return False
        

    def run(self):
        try:
            conn = pyodbc.connect(f"Driver={os.environ.get('MSSQL')};"
                          f"Server={os.environ.get('SERVER')};"
                          f"Database={os.environ.get('DB')};"
                          "Trusted_Connection=yes;")
            cursor = conn.cursor()
        except Exception as e:
            print(f"Error: {e}")
            self.logsConnection.append({self.date:e})
            
        df = self.getData()
        df["FECHA_COPIA"] = self.date

        total_registros_inical = cursor.execute(f"""SELECT COUNT(*) AS TotalRegistros
        FROM {self.table};""").fetchall()[0][0]
        print(f"Total registros inicial: {str(total_registros_inical)}")


        query_insert = """
            INSERT INTO unificado (CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, MUESTRA, VALOR, ORIGEN, FECHA_COPIA, RESULTADO)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        valores = [tuple(row) for row in df.values]
        self.insert_data(valores, query_insert, cursor)

        total_registros_post = cursor.execute(f"""SELECT COUNT(*) AS TotalRegistros
        FROM {self.table};""").fetchall()[0][0]
        print(f"Total registros final: {str(total_registros_post)}")

        cursor.commit()
        cursor.close()

        if self.checkLenLogs(self.logsConnection):
            self.writeLogs(self.logsConnection, "conn")
            pprint.pprint(self.logsConnection[-1])
        if self.checkLenLogs(self.logsDataBase):
            self.writeLogs(self.logsDataBase, "insert")
            pprint.pprint(self.logsDataBase[-1])



if __name__ == "__main__":
    table = "Unificado"
    objeto = ETL(table)
    objeto.run()
    