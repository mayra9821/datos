import pandas as pd
import numpy as np
# import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.expression import select,insert

# cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_12_2")

SQL_ALCHEMY_DATABASE_URL = 'oracle://DATOSDECAMPO:paseos@192.168.3.70:1521/sci'
# SQL_ALCHEMY_MONITOREO_URL = 'oracle://MONITOREOM:mon2007@192.168.3.70:1521/sci'
SQL_ALCHEMY_OCEANOGRAFICOS_URL = 'oracle://administrador:admin*@192.168.3.70:1521/sci'

engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
administrador = create_engine(SQL_ALCHEMY_OCEANOGRAFICOS_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base

with administrador.connect() as connection2:

    query2 = """SELECT CODIGO_SALIDA, FECHA_MUESTREO
    FROM MCODIGOS_SALIDAS 
    WHERE GRUPO = 'TH'"""
    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]    
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestreos = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestreos = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('CODIGO_SALIDA'):
        
        insertMuestreo = f"""INSERT INTO AGD_MUESTREOS (ID_MUESTREO, ID_ESTACION, ID_PROYECTO, ID_METODOLOGIA, ID_TEMATICAS, FECHA)
                        VALUES({str(857)+str(df_muestra['CODIGO_SALIDA'].values[0])},{14297}, {3492}, {857}, {213}, TO_DATE('{str(df_muestra['FECHA_MUESTREO'].values[0]).replace('T',' ').split('.')[0]}','YYYY-MM-DD HH24:MI:SS'))"""
        
        muestreos.append(insertMuestreo)

    muestreos = pd.DataFrame(data=muestreos, columns = ['SQL'])
    print(muestreos)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    muestreos.to_csv('AGD_MUESTREOS.csv', index=False)

# with engine.connect() as connection:

#     for index, row in muestreos.iterrows():
#         connection.execute(row['SQL'])
#     print('MUESTREOS AGREGADOS')

