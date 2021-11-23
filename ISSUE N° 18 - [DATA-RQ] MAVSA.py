#!/usr/bin/env python
# coding: utf-8

#Importacion de librerias
import pandas as pd
import requests
from lxml import html
pd.options.display.float_format = '{:.2f}'.format

#from alphacast import Alphacast
#with open ('ApiKey.csv', 'r') as API_key:
    #API_key = API_key.readline().strip()
#alphacast = Alphacast(API_key)


# # CREACION REPOSITORIO EN ALPHACAST:

# respositorio = alphacast.repository.create("Pendiente", repo_description="Data_Science", slug="DS"
#                                            , privacy="Public", returnIdIfExists=True)


# # CREACION DE LOS DATASETS EN ALPHACAST:

# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - Avalado- Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - Garant.- Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - No Garant. - Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - Warant - Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - PAGARÉ - Avalado - Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - PAGARÉ - No Garant. - Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - FCE - No Garant. - Diario.", xxxx, "Pendiente")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - Caucion Pesos - Diario.", xxxx, "Pendiente")


# # INICIALIZACION COLUMNAS PARA CADA UNO DE LOS 8 DATASETS CREADO

#hacer para cada dataset creado:
# alphacast.datasets.dataset(xxxx).initialize_columns(dateColumnName = "date", entitiesColumnNames=["date","plazo"],
#                                                     dateFormat= "%Y-%m-%d")

# URL y encabezados
url2 = "https://www.mav-sa.com.ar/#"
encabezados  = {"user-agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}


# Conexión con URL y parseo de html
r = requests.get(url2, headers = encabezados, verify=False)
html_doc = r.text
parser  =  html.fromstring (html_doc)
df_list = pd.read_html(html_doc)


# Obtencion dinamica de fechas para cada dataset mediante web scraping
fecha_CPD_ECHEQ = parser.xpath ("//div[@id='collapseCPD-ECHEQ']/h3/span/text()")[0][-9:].strip()
fecha_PAGARE = parser.xpath ("//div[@id='collapsePAGARE']/h3/span/text()")[0][-9:].strip()
fecha_FCE = parser.xpath ("//div[@id='collapseFCE']/h3/span/text()")[0][-9:].strip()
fecha_Caucion_Pesos = parser.xpath ("//div[@id='collapsecaucionpesos']/h3/span/text()")[0][-9:].strip()

#Obtencion de los dataframes con los datos brutos
CPD_ECHEQ_Avalado_Diario = df_list[0]
CPD_ECHEQ_Garant_Diario = df_list[1]
CPD_ECHEQ_No_Garant_Diario = df_list[2]
CPD_ECHEQ_Warant_Diario = df_list[3]
PAGARE_Avalado_Diario = df_list[4]
PAGARE_No_Garant_Diario = df_list[5]
FCE_No_Garant_Diario = df_list[6]
Caucion_Pesos_Diario = df_list[7]

# Función de limpieza y trasnformación del de cada dataset
def transform_data(df, fecha):
    try:
        df["Monto Nom."] = df["Monto Nom."].apply(lambda x: x/100)
        df["Monto Liq."] = df["Monto Liq."].apply(lambda x: x/100)
        df["date"] = pd.to_datetime(fecha)
        df.insert(0,"date", df.pop('date'))
    except:
        df["date"] = pd.to_datetime(fecha)
        df.insert(0,"date", df.pop('date'))
    return df

# Obtencion de los datasets finales
CPD_ECHEQ_Avalado_Diario = transform_data(CPD_ECHEQ_Avalado_Diario, fecha_CPD_ECHEQ)
CPD_ECHEQ_Garant_Diario = transform_data(CPD_ECHEQ_Garant_Diario, fecha_CPD_ECHEQ)
CPD_ECHEQ_No_Garant_Diario = transform_data(CPD_ECHEQ_No_Garant_Diario, fecha_CPD_ECHEQ)
CPD_ECHEQ_Warant_Diario = transform_data(CPD_ECHEQ_Warant_Diario, fecha_CPD_ECHEQ)
PAGARE_Avalado_Diario = transform_data(PAGARE_Avalado_Diario, fecha_PAGARE)
PAGARE_No_Garant_Diario = transform_data(PAGARE_No_Garant_Diario, fecha_PAGARE)
FCE_No_Garant_Diario = transform_data(FCE_No_Garant_Diario, fecha_FCE)
Caucion_Pesos_Diario = transform_data(Caucion_Pesos_Diario, fecha_Caucion_Pesos)


# # ENVIO DE LA INFORMACION A CADA UNO DE LOS FUTUROS DATASETS

# para cada dataset con su correspondiente id

# alphacast.datasets.dataset (datasetId) .upload_data_from_df (df,
# deleteMissingFromDB = False, onConflictUpdateDB = True)