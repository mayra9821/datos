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

with engine.connect() as connection:
    parametrosQuery = "SELECT ID_PARAMETRO,NOMBRE,CODIGO_LETRAS FROM AGM_PARAMETROS"
    parametrosQueryResultado = connection.execute(parametrosQuery)
    parametrosCur = parametrosQueryResultado.fetchall()
    parametrosDf = pd.DataFrame(parametrosCur)
    parametrosDf.columns = [colName.upper() for colName in parametrosQueryResultado.keys()] 
    
    unidadesQuery = "SELECT ID_UNIDAD_MEDIDA, DETALLE, DESCRIPCION FROM AGM_UNIDADES_MEDIDA"
    unidadesQueryResultado = connection.execute(unidadesQuery)
    unidadesCur = unidadesQueryResultado.fetchall()
    unidadesDf = pd.DataFrame(unidadesCur)
    unidadesDf.columns = [colName.upper() for colName in unidadesQueryResultado.keys()]
    
    metodosQuery = """SELECT DISTINCT AGM_PARAMETROS.NOMBRE PARAMETRO, AGM_METODOS.ID_METODO FROM AGM_METODOS 
                        INNER JOIN AGM_METODOSXVARIABLE ON AGM_METODOS.ID_METODO = AGM_METODOSXVARIABLE.ID_METODO 
                        INNER JOIN AGM_PARAMETROS ON AGM_METODOSXVARIABLE.ID_VARIABLE = AGM_PARAMETROS.ID_PARAMETRO
                        WHERE AGM_METODOS.ID_METODO IN (619,622,769,621,618,620,608,609)"""
    metodosQueryResultado = connection.execute(metodosQuery)
    metodosCur = metodosQueryResultado.fetchall()
    metodosDf = pd.DataFrame(metodosCur)
    metodosDf.columns = [colName.upper() for colName in metodosQueryResultado.keys()]

with monitoreo.connect() as connection2:

    query2 = "SELECT ID_MUESTREO, ID_CUALIDAD, VARIABLE, UNIDAD, VALOR_NUM  FROM VM_DATOS_MONITOREO WHERE ID_PROYECTO = 2148 AND ID_MUESTREO = 3226201409230752011 AND VARIABLE != 'Botella'"
    query2Result = connection2.execute(query2)
    datos2 = query2Result.fetchall()
    datos2Df = pd.DataFrame(datos2)
    datos2Df.columns = [colName.upper() for colName in query2Result.keys()]                                                                                                                                          
    datos2Df['COMPLEMENTO'] = datos2Df['ID_CUALIDAD'].str.extract(r'((?=\s).*)', expand = False).apply(lambda x: x.strip().replace(",","")[2:])
    datos2Df['ID_MUESTRA'] = datos2Df['ID_MUESTREO'].astype('str') + datos2Df['COMPLEMENTO'].astype('str')
    
    dfMuestrasVariables = pd.DataFrame(columns = ['ID_PARAMETRO','ID_METODOLOGIA','ID_UNIDAD_MEDIDA','ID_MUESTRA','ID_METODO','VALOR'])
    
    for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
    
        muestra = {'ID_PARAMETRO': df_muestra['VARIABLE'].values[0], 'ID_METODOLOGIA': 859, 'ID_UNIDAD_MEDIDA': df_muestra['UNIDAD'].values[0], 'ID_MUESTRA': df_muestra['ID_MUESTRA'].values[0],'ID_METODO': "",'VALOR': df_muestra['VALOR_NUM'].astype(float).values[0]}
        dfMuestrasVariables = dfMuestrasVariables.append(muestra, ignore_index=True)

    dfMuestrasVariables.loc[dfMuestrasVariables['ID_PARAMETRO'] == "Presión", 'ID_PARAMETRO'] = 'Presión columna de agua'
    dfMuestrasVariables.loc[dfMuestrasVariables['ID_PARAMETRO'] == "Salinidad", 'ID_UNIDAD_MEDIDA'] = 'PSU'
    dfMuestrasVariables.loc[dfMuestrasVariables['ID_PARAMETRO'] == "Conductividad", 'ID_UNIDAD_MEDIDA'] = 'mS/cm'
    
    for parametro in dfMuestrasVariables['ID_PARAMETRO'].unique().tolist():
    
        parametroID = parametrosDf[parametrosDf['NOMBRE'] == parametro]['ID_PARAMETRO'].values[0]
        metodoID = metodosDf[metodosDf['PARAMETRO'] == parametro]['ID_METODO'].values[0]
        dfMuestrasVariables.loc[dfMuestrasVariables['ID_PARAMETRO'] == parametro, 'ID_METODO'] = metodoID
        dfMuestrasVariables.loc[dfMuestrasVariables['ID_PARAMETRO'] == parametro, 'ID_PARAMETRO'] = parametroID

    for unidad in dfMuestrasVariables['ID_UNIDAD_MEDIDA'].unique().tolist():
    
        unidadID = unidadesDf[unidadesDf['DETALLE'] == unidad]['ID_UNIDAD_MEDIDA'].values[0]
        dfMuestrasVariables.loc[dfMuestrasVariables['ID_UNIDAD_MEDIDA'] == unidad, 'ID_UNIDAD_MEDIDA'] = unidadID   

    print(dfMuestrasVariables)  
    
    
    
    # print(dfMuestrasVariables['ID_PARAMETRO'].unique)
    # print(datos2Df['ID_MUESTRA'])
    # agd_muestras = pd.DataFrame(columns = ['ID_MUESTRA','ID_MUESTREO','NOTAS','ES_REPLICA'])
    # muestras = list()
    # print(datos2Df['ID_MUESTRA'].unique().size)
    # print(datos2Df)
    
    
    
    
    # 
    
    # for _, df_muestra in datos2Df.groupby('ID_MUESTRA'):
        
    #     insertTemperatura = f"""INSERT INTO AGD_MUESTRAS_VARIABLES (ID_PARAMETRO, ID_METODOLOGIA, ID_UNIDAD_MEDIDA, ID_MUESTRA, ID_METODO, VALOR, QUALITY_FLAG, PRESICION)
    #                     VALUES({94},{859},{4},{df_muestra['ID_MUESTRA'].values[0]}, {618}, {df_muestra['VALOR_NUM'].values[0]},{2}, {'null'})"""
        
    #     muestras.append(insertTemperatura)
        
    # muestras = pd.DataFrame(data=muestras, columns = ['SQL'])                  
    # print(muestras)
    # # print(pd.DataFrame(datos2Df['ID_MUESTRA']))
    # # pd.DataFrame(datos2Df['ID_MUESTRA']).to_csv('muestras.csv', index=False)
    # muestras.to_csv('muestras_variables_temp.csv', index=False)

# with engine.connect() as connection:

    # for index, row in insertMuestra.iterrows():
    #     connection.execute(row['SQL'])
#         print(row[])
