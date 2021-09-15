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
    
    # query = "SELECT * FROM BMUESTREOS WHERE ID_PROYECTO = 2148"
    # queryResult = connection2.execute(query)
    # datos = queryResult.fetchall()
    # datosDf = pd.DataFrame(datos)
    # datosDf.columns = [colName.upper() for colName in queryResult.keys()]

    # agd_muestreos_parametros = pd.DataFrame(columns = ['ID_MUESTREO','ID_PARAMETRO','ID_METODOLOGIA','ID_UNIDAD_MEDIDA','VALOR'])
    # inserts = list()
    
    # for df_muestreo in datosDf.groupby(['ID_MUESTREO']):
        
        # entidadMuestreos = pd.DataFrame({'ID_MUESTREO': df_muestreo['ID_MUESTREO'],'ID_PARAMETRO': 828,'ID_METODOLOGIA': 859,'ID_UNIDAD_MEDIDA': 100,'VALOR': df_muestreo['ID_ENTIDAD']})
        
        # proyectoMuestreos = pd.DataFrame({'ID_MUESTREO': df_muestreo['ID_MUESTREO'],'ID_PARAMETRO': 127,'ID_METODOLOGIA': 859,'ID_UNIDAD_MEDIDA': 100,'VALOR': df_muestreo['ID_PROYECTO']})
        
        # agd_muestreos_parametros = agd_muestreos_parametros.append([entidadMuestreos,proyectoMuestreos])
        
    #     insertEntidad = f"""INSERT INTO AGD_MUESTREOS_PARAMETROS (ID_MUESTREO,ID_PARAMETRO,ID_METODOLOGIA,ID_UNIDAD_MEDIDA,VALOR) 
    #                         VALUES ({df_muestreo['ID_MUESTREO'].values[0]},{828},{859},{100},'{'ANH'}')"""
        
    #     insertProyecto = f"""INSERT INTO AGD_MUESTREOS_PARAMETROS (ID_MUESTREO,ID_PARAMETRO,ID_METODOLOGIA,ID_UNIDAD_MEDIDA,VALOR) 
    #                         VALUES ({df_muestreo['ID_MUESTREO'].values[0]},{127},{859},{100},{df_muestreo['ID_PROYECTO'].values[0]})"""

    #     inserts.append(insertEntidad)
    #     inserts.append(insertProyecto)
    
    # inserts = pd.DataFrame(data=inserts, columns = ['SQL'])
    # print(inserts)
    # np.savetxt(r'F:\Users\ASUS\Desktop\datosBDOracle\muestreos_sql.txt', inserts.values, fmt='%s', delimiter='\t')
    # inserts.to_csv('out.csv', index=False)
        
    
    query2 = "SELECT ID_MUESTREO, ID_CUALIDAD, MUESTREOSOBSERVACIONES, REPLICA FROM VM_DATOS_MONITOREO WHERE ID_PROYECTO = 2148 "
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
        
        insertMuestra = f"""INSERT INTO AGD_MUESTRAS (ID_MUESTRA, ID_MUESTREO, NOTAS, ES_REPLICA)
                        VALUES({df_muestra['ID_MUESTRA'].values[0]},{df_muestra['ID_MUESTREO'].values[0]},{df_muestra['MUESTREOSOBSERVACIONES'].values[0]},{df_muestra['REPLICA'].values[0]})"""
        
        muestras.append(insertMuestra)
    muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    print(muestras)
    # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    muestras.to_csv('muestras.csv', index=False)

# with engine.connect() as connection:

    # for index, row in insertMuestra.iterrows():
    #     connection.execute(row['SQL'])
#         print(row[])

    # (?=\s).*
    # compression_opts = dict(method='zip', archive_name='out.csv') 
    # inserts.to_csv('out.zip', compression = compression_opts)


        
    

    # for muestreo in datosDf['ID_MUESTREO'].unique():
    #     filterData = datosDf[datos['ID_MUESTREO'] == muestreo]
    #     print(filterData)        
    
        
    # print(list(pd.unique(datosDf['ID_MUESTREO'])))

# with engine.connect() as connection:

    # for index, row in inserts.iterrows():
    #     connection.execute(row['SQL'])
    
#     for _, row in agd_muestreos_parametros.reset_index().iterrows():
        
        
#         print(row)
#         metadata_obj = MetaData()
#         # insert(AGD_MUESTREOS_PARAMETROS).values(ID_MUESTREO = agd_muestreos_parametros[row],)  
        
    # query = "SELECT * FROM AGM_PARAMETROS"
    # queryResult = connection.execute(query)
    # datos = queryResult.fetchall()
    # datosDf = pd.DataFrame(datos)
    # datosDf.columns = [colName.upper() for colName in queryResult.keys()]
    # print(datosDf['ID_PARAMETRO'])