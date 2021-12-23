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

    query2 =   query2 = """SELECT ID_MUESTREO, ID_CUALIDAD,substr(((to_number(to_char(to_date(substr(id_cualidad, 1, 8), 'YYYYMMDD'), 'J')) - 2415019)+ (to_number(substr(id_cualidad, 10, 2)) / 24)
    + (to_number(substr(id_cualidad, 12, 2)) /(60 * 24))+ (to_number(substr(id_cualidad, 14, 2)) /(3600 * 24))+((to_number(substr(id_cualidad, 17, 2)) /(100*3600 * 24)))) ,1,21)AS FECHA
    FROM BMUESTRAS_VARIABLES WHERE ID_MUESTREO IN (3010201612210000008,3010201612210000009) """

    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                                                                                          
    datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].str.extract(r'((?=\s).*)', expand = False).apply(lambda x: x.strip().replace(",","")[2:])
    datos2Df['ID_MUESTRA'] = datos2Df['ID_MUESTREO'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
        insertFecha = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, VALOR)
                        VALUES({646},{859},{100},{df_muestra['ID_MUESTRA'].values[0]}, {df_muestra['FECHA'].values[0]})"""
        
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



(33792021112310000011381,
33792021112310000011382,
33792021112310000011383,
33792021112310000011384,
33792021112310000011385,
33792021112310000011386,
33792021112310000011387,
33792021112310000011388,
33792021112310000011389,
337920211123100000113810,
337920211123100000113811,
337920211123100000113812,
337920211123100000113813,
337920211123100000113814,
337920211123100000113815,
337920211123100000113816,
337920211123100000113817,
337920211123100000113818,
337920211123100000113819,
337920211123100000113820,
337920211123100000113821,
337920211123100000113822,
337920211123100000113823,
337920211123100000113824,
337920211123100000113825,
337920211123100000113826,
337920211123100000113827,
337920211123100000113828,
337920211123100000113829,
337920211123100000113830,
337920211123100000113831,
337920211123100000113832,
337920211123100000113833)



34282021122108400011381,
34282021122108400011382,
34282021122108400011383,
34282021122108400011384,
34282021122108400011385,
34282021122108400011386,
34282021122108400011387,
34282021122108400011388,
34282021122108400011389,
342820211221084000113810,
342820211221084000113811,
342820211221084000113812,
342820211221084000113813,
342820211221084000113814,
342820211221084000113815

