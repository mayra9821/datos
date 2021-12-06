import pandas as pd
import numpy as np
import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.expression import select,insert
cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_11_2")


SQL_ALCHEMY_DATABASE_URL = 'oracle://DATOSDECAMPO:paseos@192.168.3.70:1521/sci'
SQL_ALCHEMY_MONITOREO_URL = 'oracle://MONITOREOM:mon2007@192.168.3.70:1521/sci'

engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
monitoreo = create_engine(SQL_ALCHEMY_MONITOREO_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base

with engine.connect() as connection2:

    query2 = """SELECT CASE
                WHEN substr(FECHA_HORA_REGISTRO, 21, 1) = 'p' THEN
                    id_muestreo
                    || replace(replace(replace(substr(FECHA_HORA_REGISTRO, 1, 11)
                                            ||(to_number(substr(FECHA_HORA_REGISTRO, 12, 2)) + 12)
                                            || substr(FECHA_HORA_REGISTRO, 14, 3), ':'), '/'), ' ')
                WHEN substr(FECHA_HORA_REGISTRO, 21, 1) = 'a' THEN
                    id_muestreo
                    || replace(replace(replace(substr(FECHA_HORA_REGISTRO, 1, 16), ':'), '/'), ' ')
                ELSE
                    id_muestreo
                    || replace(replace(replace(FECHA_HORA_REGISTRO || ' 00:00', ':'), '/'), ' ')
                END AS ID_MUESTRA, VALOR
                FROM VM_AGM_2507_816
                WHERE  ID_MUESTREO IN (81620180118151515,81620180119151148,81620180122132412,
                81620180122160828,81620180411161602,81620180914081944,81620181130080509,81620181228134507,81620190514144057,81620190813084908) AND UNIDADES IN (66) """
    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                    
    # datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].str.extract(r'((?=).*)', expand = False).apply(lambda x: x.strip().replace(" ","")[3:11])
    # datos2Df['ID_MUESTRA'] = str(803)+datos2Df['ID_MUESTREO'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
        insertVA = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, ID_METODO, VALOR) 
        VALUES({142},{803},{66},{df_muestra['ID_MUESTRA'].values[0]}, {667}, {df_muestra['VALOR'].values[0]})"""
        
        muestras.append(insertVA)
        
    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('muestras_variables_VA.csv', index=False)

# with engine.connect() as connection:

#     for index, row in muestras.iterrows():
#         connection.execute(row['SQL'])
#     print('MUESTRAS AGREGADAS')
