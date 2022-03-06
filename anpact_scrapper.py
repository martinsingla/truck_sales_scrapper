##############################################################
# scrap_last_ANPACT_records()
# Param: None
# Output: pandas dataframe w/ 1 row containing latest ANPACT report data

# update_ANPACTdb_last_records()
# Param: None
# Output: Updates Mongo database in case new ANPACT data identified

# get_ANPACTdb_full_data()
# Param: None
# Output: Full dataset w/ historic CV sales data
##############################################################

import pandas as pd
import numpy as np
import datetime as dt
import tabula
import pymongo
from credentials import mongodb_user, mongodb_password, mongodb_cluster, mongodb_database

def scrap_last_ANPACT_records():
    
    url= 'https://www.anpact.com.mx/documentos/anpact/BOLETIN-ESTADISTICO-ANPACT.pdf' 
    dfs= tabula.read_pdf(url, pages='1')
    
    #Identificar Año del último reporte publicado
    y= dfs[0].iloc[0,0]
    #Identificar Mes del año del último reporte publicado
    meses= {'Enero' : 1,'Febrero': 2,'Marzo': 3,'Abril': 4,'Mayo': 5,'Junio': 6,
            'Julio': 7,'Agosto': 8,'Septiembre': 9, 'Octubre': 10,'Noviembre': 11,'Diciembre': 12}
    m= meses[list(meses.keys())[[mes in dfs[0].columns[0] for mes in list(meses.keys())].index(True)]]
    #Fecha del ultimo reporte
    fecha= pd.to_datetime(str(y) + '-' + str(m))
    
    clases_camiones = ['Fecha', 'truck4_5_ANPACT', 'truck6', 'truck7', 'truck8', 'truckTractor']
    ventas_camiones_mes_año_actual = np.append([fecha, 
                                                dfs[2].iloc[13:15, 0].astype(int).sum()], #suma de clase 4 y 5
                                               dfs[2].iloc[15:19, 0]) #otras clases
    menudeo_camiones= pd.DataFrame(np.array([ventas_camiones_mes_año_actual]), columns= clases_camiones)
    
    #Datos buses menudeo
    clases_buses = ['Fecha', 'bus5_6', 'bus7', 'bus8', 'busLongDist']
    ventas_buses_mes_año_actual = np.append([fecha], dfs[2].iloc[23:27, 0])
    menudeo_buses= pd.DataFrame(np.array([ventas_buses_mes_año_actual]), columns= clases_buses)
    
    #Datos camiones y buses
    menudeo = pd.merge(menudeo_camiones, menudeo_buses, on= 'Fecha')
    for col in menudeo.columns[1:]:
        menudeo[col] = menudeo[col].astype(str).str.replace(',', '').astype(float)
    
    return menudeo

def update_ANPACTdb_last_records():
    
    #Set connection with MongoDB
    conn = f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_cluster}.qf8nk.mongodb.net/{mongodb_database}?retryWrites=true&w=majority'
    client = pymongo.MongoClient(conn)
    db = client.mexican_truckDB
    
    #Scrap latest ANPACT reports
    new_record = scrap_last_ANPACT_records()

    #Get last data point from the databse
    last_record = db.sales.find_one(sort=[( '_id', pymongo.DESCENDING )])
    last_record = pd.to_datetime(last_record['date'])

    #Check if there is new data to be appended to database
    if last_record == new_record['Fecha'][0]:
        print('Most recent ANPACT data already recorded in database! :)')

    elif last_record == new_record['Fecha'][0] - pd.DateOffset(month=1):
        print('New ANPACT report published. Appending new data to database! :)')

        db.sales.insert_one({
            'date': new_record['Fecha'][0],
            'sales': {
                'truck4_5_ANPACT': new_record['truck4_5_ANPACT'][0],
                'truck6': new_record['truck6'][0],
                'truck7': new_record['truck7'][0],
                'truck8': new_record['truck8'][0],
                'truckTractor': new_record['truckTractor'][0],
                'bus5_6': new_record['bus5_6'][0],
                'bus7': new_record['bus7'][0],
                'bus8': new_record['bus8'][0],
                'busLongDist': new_record['busLongDist'][0]
            },
            'date_added' : str(dt.datetime.now()).replace('-', '').replace(' ', '').replace(':', '')[:-7]
            })    

    elif last_record != new_record['Fecha'][0] - pd.DateOffset(month=1):
        print('New ANPACT report published, but last record does not match new record.')
        print('You may have skipped scrapping last month´s report... :O')
        
    client.close()
    
    return print('----')

def get_ANPACTdb_full_data():
    
    #Set connection with MongoDB
    conn = f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_cluster}.qf8nk.mongodb.net/{mongodb_database}?retryWrites=true&w=majority'
    client = pymongo.MongoClient(conn)
    db = client.mexican_truckDB
    
    #get records from database
    records= db.sales.find()
    records= list(records)

    #cnovert and format in pandas dataframe
    records_dic={}
    for i in range(0, len(records)):
        record= records[i]['sales']
        record['date'] = records[i]['date']
        records_dic[i] = record

    data= pd.DataFrame.from_dict(records_dic, orient= 'index')
    data= data[np.append(['date'], data.columns[:-1])]
    
    client.close()
    
    return data