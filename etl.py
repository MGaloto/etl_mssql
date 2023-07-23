import pandas as pd
import pyodbc 
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

class ETL():
    """
    En esta clase se insertan los datos del csv en mssql la tabla Unificado.
    """
    def __init__(self, table):
        self.table = table
        
    def getData(self):
        data = pd.read_csv(
            'https://adlssynapsetestfrancis.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2023-04-20T15:25:12Z&se=2023-12-31T23:25:12Z&spr=https&sv=2021-12-02&sr=b&sig=MZIobvBY6c7ht%2FdFLhtyJ3MZgqa%2B75%2BY3YWntqL%2FStI%3D')
        return data
    
    def getDateNow(self):
        fecha_hora_actual = datetime.now()
        formato = "%Y-%m-%d %H:%M:%S.%f"
        date = fecha_hora_actual.strftime(formato)[:-3]
        return date
        

    def run(self):
        try:
            conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          f"Server={os.environ.get('SERVER')};"
                          f"Database={os.environ.get('DB')};"
                          "Trusted_Connection=yes;")
            cursor = conn.cursor()
        except Exception  as e:
            print(f"Error: {e}")
            
        df = self.getData()
        df["FECHA_COPIA"] = self.getDateNow()
        
        total_registros_inical = cursor.execute(f"""SELECT COUNT(*) AS TotalRegistros
        FROM {self.table};""").fetchall()[0][0]
        print(f"Total registros inicial: {str(total_registros_inical)}")


        query_insert = """
            INSERT INTO unificado (CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, MUESTRA, VALOR, ORIGEN, FECHA_COPIA, RESULTADO)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Se convierte el DataFrame a una lista de tuplas para usar con executemany
        valores = df.values.tolist()
        cursor.executemany(query_insert, valores)


        total_registros_post = cursor.execute(f"""SELECT COUNT(*) AS TotalRegistros
        FROM {self.table};""").fetchall()[0][0]
        print(f"Total registros final: {str(total_registros_post)}")

        cursor.commit()
        cursor.close()



if __name__ == "__main__":
    table = "Unificado"
    objeto = ETL(table)
    objeto.run()
    
    
    
    