import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.expression import select,insert


SQL_ALCHEMY_DATABASE_URL = 'oracle://DATOSDECAMPO:paseos@192.168.3.70:1521/sci'
SQL_ALCHEMY_MONITOREO_URL = 'oracle://MONITOREOM:mon2007@192.168.3.70:1521/sci'

engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
monitoreo = create_engine(SQL_ALCHEMY_MONITOREO_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base

with monitoreo.connect() as connection2:

    query2 = """SELECT ID_MUESTREO, ID_CUALIDAD, VALOR_NUM  FROM VM_DATOS_MONITOREO WHERE ID_PROYECTO = 3280 AND COD_VARIABLE = 'CLA' 
    AND ID_MUESTREO in (3280201612210000001, 3280201612210000002, 3280201612210000003, 3280201612210000004, 3280201612210000005, 3280201612210000006,3280201612210000007,
                3280201612210000008, 3280201612210000009, 32802016122100000010, 32802016122100000011, 32802016122100000012, 32802016122100000013, 32802016122100000014, 
                32802016122100000015, 32802016122100000016, 32802016122100000017,32802016122100000018, 32802016122100000019, 32802016122100000020, 32802016122100000021,
                32802016122100000022, 32802016122100000023, 32802016122100000024, 32802016122100000025, 32802016122100000026, 32802016122100000027, 32802016122100000028,
                32802016122100000029, 32802016122100000030) """
    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                                                                                          
    datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].str.extract(r'((?=\s).*)', expand = False).apply(lambda x: x.strip().replace(".","")[2:])
    datos2Df['ID_MUESTRA'] = datos2Df['ID_MUESTREO'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
        insertClorofila = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, ID_METODO, VALOR)
                        VALUES({8},{859},{13},{df_muestra['ID_MUESTRA'].values[0]}, {609}, {df_muestra['VALOR_NUM'].values[0]})"""
        
        muestras.append(insertClorofila)
        
    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('muestras_variables_clorofila.csv', index=False)

with engine.connect() as connection:

    for index, row in muestras.iterrows():
        connection.execute(row['SQL'])
    print('MUESTRAS AGREGADAS')