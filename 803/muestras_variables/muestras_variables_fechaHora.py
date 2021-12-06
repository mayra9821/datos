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
    # SELECT ID_MUESTREO, ID_CUALIDAD, to_number(substr(to_char((to_number(to_char(to_date(substr(id_cualidad, 1, 8), 'YYYYMMDD'), 'J')) - 2415019 ) + to_number(substr(id_cualidad, 10, 2)) / 24 +
    #             to_number(substr(id_cualidad, 12, 2)) / ( 60 * 24 ) + to_number(substr(id_cualidad, 14, 5)) / ( 3600 * 24 )),1,21)) AS FECHA
    #             FROM BMUESTRAS_VARIABLES WHERE ID_MUESTREO IN(3139201010051436461)

    # SELECT ID_MUESTREO, ID_CUALIDAD, to_number(substr(to_char((to_number(to_char(to_date(substr(FECHA, 1, 8), 'YYYYMMDD'), 'J')) - 2415019 ) + to_number(substr(FECHA, 10, 2)) / 24 +
    #             to_number(substr(FECHA, 12, 2)) / ( 60 * 24 ) + to_number(substr(FECHA, 14, 5)) / ( 3600 * 24 )),1,21)) AS FECHA
    #             FROM BMUESTRAS_VARIABLES WHERE ID_MUESTREO IN(3139201010051436461)
    query2 = """SELECT CASE
                    WHEN substr(fecha_hora, 21, 1) = 'p' THEN
                        id_muestreo
                        || replace(replace(replace(substr(fecha_hora, 1, 11)
                        ||(to_number(substr(fecha_hora, 12, 2)) + 12)
                        || substr(fecha_hora, 14, 3), ':'), '/'), ' ')
                    WHEN substr(fecha_hora, 21, 1) = 'a' THEN
                        id_muestreo
                        || replace(replace(replace(substr(fecha_hora, 1, 16), ':'), '/'), ' ')
                    ELSE
                        id_muestreo
                        || replace(replace(replace(fecha_hora || ' 00:00', ':'), '/'), ' ')
                    END AS ID_MUESTRA,
                    (CASE
                    WHEN substr(fecha_hora, 21, 1) = 'p'
                    AND NOT substr(fecha_hora, 12, 2) = '12' THEN
                    to_number(substr(to_char((to_number(to_char(to_date(substr(fecha_hora, 1, 10), 'DD/MM/YYYY'), 'J')) - 2415019) -- Fecha a numero
                    +((to_number(substr(fecha_hora, 12, 2)) + 12) / 24) -- Hora a numero
                    + to_number(substr(fecha_hora, 15, 2)) /(60 * 24) -- Minuto a numero
                    + to_number(substr(fecha_hora,
                    18, 2)) /(3600 * 24) -- Segundos a numero
                    ),1,21))
                    WHEN substr(fecha_hora, 21, 1) = 'a'
                    OR substr(fecha_hora, 12, 2) = '12' THEN
                    to_number(substr(to_char((to_number(to_char(to_date(substr(fecha_hora, 1, 10), 'DD/MM/YYYY'), 'J')) - 2415019)-- Fecha a numero
                    +((to_number(
                    substr(fecha_hora, 12, 2))) / 24) -- Hora a numero
                    + to_number(substr(fecha_hora, 15, 2)) /(60 * 24) -- Minuto a numero
                    + to_number(substr(fecha_hora, 18,
                    2)) /(3600 * 24) -- Segundos a numero
                    ),1,21))
                    ELSE
                    to_number(substr(to_char(to_number(to_char(to_date(substr(fecha_hora, 1, 10), 'DD/MM/YYYY'), 'J')) - 2415019), 1, 21))
                    END) AS FECHA_ARREGLADA
                    FROM
                    vm_agm_2507_816
                    WHERE VARIABLE IS NULL AND ID_MUESTREO IN (81620180723105032) AND UNIDADES IN (45,66,67,43) AND FECHA_HORA LIKE ('%/2018%')"""


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
        
        insertFecha = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, VALOR)
                        VALUES({646},{803},{100},{df_muestra['ID_MUESTRA'].values[0]}, '{df_muestra['FECHA_ARREGLADA'].values[0]}')"""
        
        muestras.append(insertFecha)

    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('muestras_variables_fechaHora.csv', index=False)

# with engine.connect() as connection:

#     for index, row in muestras.iterrows():
#         connection.execute(row['SQL'])
#     print('MUESTRAS AGREGADAS')    

