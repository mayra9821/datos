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

    query2 = """SELECT FECHA_MUESTREO,TO_CHAR(FECHA_MUESTREO,'YYYYMMDD HH24MISS') AS ID_CUALIDAD, CODIGO_SALIDA,
                (TO_NUMBER(TO_CHAR(FECHA_MUESTREO, 'J'))-2415019)+ (TO_NUMBER(TO_CHAR(FECHA_MUESTREO, 'HH24')) / 24 )
                + (TO_NUMBER(TO_CHAR(FECHA_MUESTREO, 'MI')) / ( 60 * 24 )) + (TO_NUMBER(TO_CHAR(FECHA_MUESTREO,'SS')) / ( 3600 * 24 )) FECHA_HORA
                FROM MHOBOS
                WHERE CODIGO_SALIDA IN (
                        359,360,361,362,363,364,365,366,367,368,369,370,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,
                        392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,
                        424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,443,444,445,446,447,448,449,450,451,452,453,454,455,456,
                        457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,475,476,477,478,479,480,481) """


    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                                                                                          
    datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].apply(lambda x: x.strip().replace(" ","")[4:])
    ##str.extract(r'((?=\s).*)', expand = False).   replace("/","").replace(":","").replace("null","")
    datos2Df['ID_MUESTRA'] = datos2Df['CODIGO_SALIDA'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
        insertFecha = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, VALOR)
                        VALUES({646},{857},{100},{df_muestra['ID_MUESTRA'].values[0]}, {df_muestra['FECHA_HORA'].values[0]})"""
        
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
