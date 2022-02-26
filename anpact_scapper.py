import pandas as pd
import numpy as np
import os
import tabula
import datetime as dt

def update_ANPACT():

    ## Buscar datos de tablas reporte ANPACT mensual
    ## verificar que el reporte se siga publicando en la misma liga
    ## verificar que las tablas en el reporte guarden exactamente el mismo formato!

    url= 'https://www.anpact.com.mx/documentos/anpact/BOLETIN-ESTADISTICO-ANPACT.pdf' 
    dfs= tabula.read_pdf(url, pages='1')

    ## Historico ANPACT
    #Buscar el archivo más reciente (última actualización!)

    timekey= max([os.path.getctime('01_HISTORICOS/' + i) for i in os.listdir('01_HISTORICOS/')])
    index= [os.path.getctime('01_HISTORICOS/' + i) for i in os.listdir('01_HISTORICOS/')].index(timekey)
    dir= os.listdir('01_HISTORICOS/')[index]

    #Abrir histórico!
    hist= pd.read_csv('01_HISTORICOS/'+dir)
    hist['Fecha']= pd.to_datetime(hist['Fecha'])

    #Identificar Año del último reporte publicado
    y= dfs[0].iloc[0,0]

    #Identificar Mes del año del último reporte publicado
    meses= {'Enero' : 1,'Febrero': 2,'Marzo': 3,'Abril': 4,'Mayo': 5,'Junio': 6,
            'Julio': 7,'Agosto': 8,'Septiembre': 9, 'Octubre': 10,'Noviembre': 11,'Diciembre': 12}
    m= meses[list(meses.keys())[[mes in dfs[0].columns[0] for mes in list(meses.keys())].index(True)]]

    #Fecha del ultimo reporte
    fecha= pd.to_datetime(str(y) + '-' + str(m))

    #### Chequear que la última fecha del dataframe NO 
    ultimo_registro = hist.iloc[-1, 0]

    if (fecha != ultimo_registro) & (ultimo_registro == fecha - pd.DateOffset(months=1)):
        
        print('-----')
        print('Última fecha de registro correcta')
        print('Proceder a agregar nuevos registros...')

        #Datos camiones menudeo
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
        menudeo

        #Nuevo dataframe con última actualización
        new= hist.append(menudeo).reset_index(drop=True)
        
        # Guardar nuevo dataframe con fecha
        now= str(dt.datetime.now()).replace('-', '').replace(' ', '').replace(':', '')[:-7]
        new.to_csv('01_HISTORICOS/FROST_ventas_historicas_camiones_'+ now +'.csv', index=False)
        
    elif (fecha == ultimo_registro):
        print('-----')
        print('Último registro en base de datos igual al de reporte de mes en curso.')
        print('No se agregan nuevos datos')
        
    elif (fecha != ultimo_registro) & (ultimo_registro != fecha - pd.DateOffset(months=1)):
        print('-----')
        print('Es probable que no haya registrado ventas del mes anterior!! :O')

    return print('----')

update_ANPACT()