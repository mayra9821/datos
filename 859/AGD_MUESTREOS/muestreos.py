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

    query2 = """SELECT ID_MUESTREO, ID_ESTACION, ID_PROYECTO, FECHA_HORA FROM BMUESTREOS WHERE ID_PROYECTO = 3023 AND ID_COMPONENTE = 69 """
    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]    
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTREO'):
        
        insertConductividad = f"""INSERT INTO AGD_MUESTREOS (ID_MUESTREO, ID_ESTACION, ID_PROYECTO, ID_METODOLOGIA, ID_TEMATICAS, FECHA)
                        VALUES({df_muestra['ID_MUESTREO'].values[0]},{df_muestra['ID_ESTACION'].values[0]}, {143}, {859}, {251}, TO_DATE('{str(df_muestra['FECHA_HORA'].values[0]).replace('T',' ').split('.')[0]}','YYYY-MM-DD HH24:MI:SS'))"""
        
        muestras.append(insertConductividad)

    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('AGD_MUESTREOS.csv', index=False)

with engine.connect() as connection:

    for index, row in muestras.iterrows():
        connection.execute(row['SQL'])
    print('MUESTREOS AGREGADOS')