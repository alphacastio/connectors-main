#IMPORTACION LIBRERIAS

import pandas as pd
import requests
from lxml import html
from datetime import datetime
pd.options.display.float_format = '{:.2f}'.format

from alphacast import Alphacast
with open ('ApiKey.csv', 'r') as API_key:
    API_key = API_key.readline().strip()
alphacast = Alphacast(API_key)

# CREACION FUNCIONES PARA CREAR Y ENVIAR INFO A ALPHACAST

def create_dataset(name, id_repo, name_repo):
    dataset = alphacast.datasets.create(name, id_repo, name_repo)
    return dataset

def send_info(df, id_dataset):
    send = alphacast.datasets.dataset(id_dataset).upload_data_from_df (df, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True, uploadIndex=False)
    return send

# CREACION FUNCION PARA ASOCIAR SEMANA DEL AÑO AL MES
def mes(x):
    x_new = x/4.3
    x_new = int(x_new)
    if x_new==0:
        x_new = 1
    return x_new

# DEFINICION DE URL DINAMICA Y ACCESO A LOS LINKS CORRESPONDIENTES MEDIANTE WEB SCRAPING
url = "https://www.bolsadecereales.com/datasets"
encabezados  = {"user-agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}
r = requests.get(url, headers = encabezados, verify=False)
html_doc = r.text
parser  =  html.fromstring (html_doc)
url_previa = "https://www.bolsadecereales.com/"
datasets = parser.xpath ("//div[@class='enlaces-archivo-descargable']/a/@href")[2:6]

# ARMADO DEL DATASET
trigo = pd.read_csv(url_previa+datasets[0], encoding='latin-1', sep=";")
trigo["Mes"] = trigo["Semana"].apply(lambda x: mes(x))
trigo["Año"] = trigo["Campaña"].apply(lambda x: x[0:4])
trigo["Fecha"] = trigo["Año"].astype(str) + "-" + trigo["Mes"].astype(str)
trigo.insert(0, 'Fecha', trigo.pop('Fecha'))
trigo = trigo.iloc[:,0:-2]
trigo["Fecha"] = pd.to_datetime(trigo["Fecha"])

# CREACION DATASET
# create_dataset("Agricultura - Argentina - Bolsa de Cereales - Estado y Condición de Cultivos (ECC) - Trigo", 1505, "Prueba")

# INICIALIZACION DE ENTIDADES
# alphacast.datasets.dataset(8737).initialize_columns(dateColumnName = "Fecha", entitiesColumnNames=['Cultivo', 'Campaña', 'Semana', 'Zona'],
#                                                             dateFormat= "%Y-%m-%d")

# ENVIO DE INFORMACION A ALPHACAST
# send_info(trigo, 8737)


# ## DATASET Maiz
maiz = pd.read_csv(url_previa+datasets[1], encoding='latin-1', sep=";")
maiz["Mes"] = maiz["Semana"].apply(lambda x: mes(x))
maiz["Año"] = maiz["Campaña"].apply(lambda x: x[0:4])
maiz["Fecha"] = maiz["Año"].astype(str) + "-" + maiz["Mes"].astype(str)
maiz.insert(0, 'Fecha', maiz.pop('Fecha'))
maiz = maiz.iloc[:,0:-2]
maiz["Fecha"] = pd.to_datetime(maiz["Fecha"])
maiz = maiz.drop_duplicates(subset= ['Fecha','Cultivo', 'Campaña', 'Semana', 'Zona'])

# CREACION DATASET
# create_dataset("Agricultura - Argentina - Bolsa de Cereales - Estado y Condición de Cultivos (ECC) - Maíz", 1505, "Prueba")

# INICIALIZACION DE ENTIDADES
# alphacast.datasets.dataset(8739).initialize_columns(dateColumnName = "Fecha", entitiesColumnNames=['Cultivo', 'Campaña', 'Semana', 'Zona'],
#                                                             dateFormat= "%Y-%m-%d")

# ENVIO DE INFORMACION A ALPHACAST
# send_info(maiz, 8739)


# ## DATASET Soja
soja = pd.read_csv(url_previa+datasets[2], encoding='latin-1', sep=";")
soja["Mes"] = soja["Semana"].apply(lambda x: mes(x))
soja["Año"] = soja["Campaña"].apply(lambda x: x[0:4])
soja["Fecha"] = soja["Año"].astype(str) + "-" + soja["Mes"].astype(str)
soja.insert(0, 'Fecha', soja.pop('Fecha'))
soja = soja.iloc[:,0:-2]
soja["Fecha"] = pd.to_datetime(soja["Fecha"])

# CREACION DATASET
# create_dataset("Agricultura - Argentina - Bolsa de Cereales - Estado y Condición de Cultivos (ECC) - Soja", 1505, "Prueba")

# INICIALIZACION DE ENTIDADES
# alphacast.datasets.dataset(8740).initialize_columns(dateColumnName = "Fecha", entitiesColumnNames=['Cultivo', 'Campaña', 'Semana', 'Zona'],
#                                                             dateFormat= "%Y-%m-%d")

# ENVIO DE INFORMACION A ALPHACAST
# send_info(soja, 8740)


# ## DATASET Girasol
girasol = pd.read_csv(url_previa+datasets[3], encoding='latin-1', sep=";")
girasol["Mes"] = girasol["Semana"].apply(lambda x: mes(x))
girasol["Año"] = girasol["Campaña"].apply(lambda x: x[0:4])
girasol["Fecha"] = girasol["Año"].astype(str) + "-" + girasol["Mes"].astype(str)
girasol.insert(0, 'Fecha', girasol.pop('Fecha'))
girasol = girasol.iloc[:,0:-2]
girasol["Fecha"] = pd.to_datetime(girasol["Fecha"])

# CREACION DATASET
# create_dataset("Agricultura - Argentina - Bolsa de Cereales - Estado y Condición de Cultivos (ECC) - Girasol", 1505, "Prueba")

# INICIALIZACION DE ENTIDADES
# alphacast.datasets.dataset(8741).initialize_columns(dateColumnName = "Fecha", entitiesColumnNames=['Cultivo', 'Campaña', 'Semana', 'Zona'],
#                                                            dateFormat= "%Y-%m-%d")

# ENVIO DE INFORMACION A ALPHACAST
# send_info(girasol, 8741)