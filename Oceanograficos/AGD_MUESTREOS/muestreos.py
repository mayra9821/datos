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
    WHERE CODIGO_SALIDA IN (668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,696,697,698,699,
    700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,
    732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,761,762,763,
    764,765,766,767,768,769,770,771,772,773,774,775,776,777,778,779,780,781,782,783,784,785,786,787,788,789,790,791,792,793,794,795,
    796,797,798,799,800,801,802,803,804,805,806,807,808,809,810,811,812,813,814,815,816,817,818,819,820,821,822,823,824,825,826,827,
    828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,
    860,861,862,863,864,865,866,867)"""
    
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
                        VALUES({str(857)+str(df_muestra['CODIGO_SALIDA'].values[0])},{14297}, {3492}, {857}, {217}, TO_DATE('{str(df_muestra['FECHA_MUESTREO'].values[0]).replace('T',' ').split('.')[0]}','YYYY-MM-DD HH24:MI:SS'))"""
        ###TODO: ya se cambio la estacion a bunta betin y tambien la tematica  14301
        ##ID_ESTACION CHENGUE: 14297
        
        muestreos.append(insertMuestreo)

    muestreos = pd.DataFrame(data=muestreos, columns = ['SQL'])
    print(muestreos)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    muestreos.to_csv('AGD_MUESTREOS.csv', index=False)

# with engine.connect() as connection:

#     for index, row in muestreos.iterrows():
#         connection.execute(row['SQL'])
#     print('MUESTREOS AGREGADOS')


