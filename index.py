from sqlalchemy import create_engine
import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker
from sqlalchemy.sql.expression import select


SQL_ALCHEMY_DATABASE_URL = 'oracle://DATOSDECAMPO:paseos@192.168.3.70:1521/sci'
SQL_ALCHEMY_MONITOREO_URL = 'oracle://MONITOREOM:mon2007@192.168.3.70:1521/sci'

engine = create_engine(SQL_ALCHEMY_DATABASE_URL)
monitoreo = create_engine(SQL_ALCHEMY_MONITOREO_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base

with monitoreo.connect() as connection2:
    
    query = "SELECT * FROM BMUESTREOS WHERE ID_PROYECTO = 2148"
    queryResult = connection2.execute(query)
    datos = queryResult.fetchall()
    datosDf = pd.DataFrame(datos)
    datosDf.columns = [colName.upper() for colName in queryResult.keys()]
    
    agd_muestreos_parametros = pd.DataFrame(columns = ['ID_MUESTREO','ID_PARAMETRO','ID_METODOLOGIA','ID_UNIDAD_MEDIDA','VALOR','ID_MUESTREOTEX'])
    
    for _, df_muestreo in datosDf.groupby(['ID_MUESTREO']):
        
    #     entidadMuestreos = pd.DataFrame({'ID_MUESTREO': df_muestreo['ID_MUESTREO'],'ID_PARAMETRO': 828,'ID_METODOLOGIA': 859,'ID_UNIDAD_MEDIDA': 100,'VALOR': df_muestreo['ID_ENTIDAD'],
    #                                     'ID_MUESTREOTEX': df_muestreo['ID_MUESTREO']})
        
    #     proyectoMuestreos = pd.DataFrame({'ID_MUESTREO': df_muestreo['ID_MUESTREO'],'ID_PARAMETRO': 127,'ID_METODOLOGIA': 859,'ID_UNIDAD_MEDIDA': 100,'VALOR': df_muestreo['ID_PROYECTO'],
    #                                     'ID_MUESTREOTEX': df_muestreo['ID_MUESTREO']})
        
        insertEntidad = f"""INSERT INTO AGD_MUESTREOS_PARAMETROS ('ID_MUESTREO','ID_PARAMETRO','ID_METODOLOGIA','ID_UNIDAD_MEDIDA','VALOR','ID_MUESTREOTEX') 
                            VALUES ({df_muestreo['ID_MUESTREO'].values[0]},{828},{859},{100},'{str(df_muestreo['ID_ENTIDAD'].values[0])}','{str(df_muestreo['ID_MUESTREO'].values[0])}')"""
        
        insertProyecto = f"""INSERT INTO AGD_MUESTREOS_PARAMETROS ('ID_MUESTREO','ID_PARAMETRO','ID_METODOLOGIA','ID_UNIDAD_MEDIDA','VALOR','ID_MUESTREOTEX') 
                            VALUES ({df_muestreo['ID_MUESTREO']},{127},{859},{100},{df_muestreo['ID_PROYECTO']},{df_muestreo['ID_MUESTREO']})"""
                            
        print(insertEntidad)
        print(insertProyecto)
        # agd_muestreos_parametros = agd_muestreos_parametros.append([entidadMuestreos, proyectoMuestreos])
    
    # query2 = "SELECT ID_MUESTREO, ID_CUALIDAD FROM VM_DATOS_MONITOREO WHERE ID_PROYECTO = 2148"
    # query2Result = connection2.execute(query2)
    # datos2 = query2Result.fetchall()
    # datos2Df = pd.DataFrame(datos2)
    # datos2Df.columns = [colName.upper() for colName in query2Result.keys()]
    # datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].str.extract(r'((?=\s).*)', expand = False).apply(lambda x: x.strip().replace(",",""))
    # datos2Df['ID_MUESTRA'] = datos2Df['ID_MUESTREO'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    # print(datos2Df)
    
    # (?=\s).*
    
    # compression_opts = dict(method='zip', archive_name='out.csv') 
    
    # agd_muestreos_parametros.to_csv('out.zip', compression = compression_opts)


        
    

    # for muestreo in datosDf['ID_MUESTREO'].unique():
    #     filterData = datosDf[datos['ID_MUESTREO'] == muestreo]
    #     print(filterData)        
    
        
    # print(list(pd.unique(datosDf['ID_MUESTREO'])))

# with engine.connect() as connection:
    
#     query = "SELECT * FROM AGM_PARAMETROS"
#     queryResult = connection.execute(query)
#     datos = queryResult.fetchall()
#     datosDf = pd.DataFrame(datos)
#     datosDf.columns = [colName.upper() for colName in queryResult.keys()]
#     print(datosDf['ID_PARAMETRO'])