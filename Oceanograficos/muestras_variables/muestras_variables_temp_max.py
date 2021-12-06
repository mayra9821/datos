import pandas as pd
import numpy as np
# import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.expression import select,insert
# cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_11_2")


SQL_ALCHEMY_DATABASE_URL = 'oracle://DATOSDECAMPO:paseos@192.168.3.70:1521/sci'
# SQL_ALCHEMY_MONITOREO_URL = 'oracle://MONITOREOM:mon2007@192.168.3.70:1521/sci'
SQL_ALCHEMY_OCEANOGRAFICOS_URL = 'oracle://administrador:admin*@192.168.3.70:1521/sci'


engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
# monitoreo = create_engine(SQL_ALCHEMY_MONITOREO_URL)
administrador = create_engine(SQL_ALCHEMY_OCEANOGRAFICOS_URL)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base

with administrador.connect() as connection2:
    query2 = """SELECT
                CODIGO_SALIDA,
                TO_CHAR(fecha_muestreo,'YYYYMMDD HH24MISS') AS ID_CUALIDAD, TEMP_MAXIMA  
                FROM
                  MAMBIENTAL_METEREOLOGICO
                WHERE
                    CODIGO_SALIDA IN (299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,
                    328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358)"""

    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                                                                                          
    datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].apply(lambda x: x.strip().replace(" ","")[4:])
    ##.str.extract(r'((?=\s).*)', expand = False)
    datos2Df['ID_MUESTRA'] = datos2Df['CODIGO_SALIDA'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    muestras = list()
    ##print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
        insertTemperatura = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, ID_METODO, VALOR)
                        VALUES({485},{857},{5},{df_muestra['ID_MUESTRA'].values[0]}, {624}, {df_muestra['TEMP_MAXIMA'].values[0]})"""
        
        muestras.append(insertTemperatura)
        
    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)

    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('muestras_variables_temp.csv', index=False)

# with engine.connect() as connection:

#     for index, row in muestras.iterrows():
#         connection.execute(row['SQL'])
#     print('MUESTRAS AGREGADAS')