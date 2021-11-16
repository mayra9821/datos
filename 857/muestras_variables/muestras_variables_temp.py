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

with monitoreo.connect() as connection2:

    query2 = """SELECT ID_MUESTREO, ID_CUALIDAD, VALOR_NUM FROM VM_DATOS_MONITOREO WHERE ID_PROYECTO = 1480 
                AND VARIABLE='Temperatura del aire' 
                AND ID_MUESTREO IN(472)"""
                ##,460,462,454,472,,466,465,463,457,448,459,475,461,467,471,468,451,455,453,452,469,456,464,470,449
                
    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                                                                                          
    datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].apply(lambda x: x.strip().replace(" ","").replace("/","").replace(":","").replace("null","")[0:])
    ##str.extract(r'((?=\s).*)', expand = False).
    datos2Df['ID_MUESTRA'] = datos2Df['ID_MUESTREO'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
        insertTemperatura = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, ID_METODO, VALOR)
                        VALUES({484},{857},{5},{df_muestra['ID_MUESTRA'].values[0]}, {624}, {df_muestra['VALOR_NUM'].values[0]})"""
        
        muestras.append(insertTemperatura)
    
    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('muestras_variables_temp.csv', index=False)

# with engine.connect() as connection:

#     for index, row in muestras.iterrows():
#         connection.execute(row['SQL'])
#     print('Muestras agregadas')
