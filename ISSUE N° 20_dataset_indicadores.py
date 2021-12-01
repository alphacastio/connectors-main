# ### Actividad - Argentina - IPCVA - Producción - Serie de Indicadores

#Importacion de librerias
import pandas as pd
import requests
from lxml import html
from datetime import datetime
import numpy as np
pd.options.display.float_format = '{:.2f}'.format


from alphacast import Alphacast
with open ('ApiKey.csv', 'r') as API_key:
    API_key = API_key.readline().strip()
alphacast = Alphacast(API_key)

#respositorio = alphacast.repository.create("Prueba", repo_description="Prueba", slug="Prueba"
#                                             , privacy="Public", returnIdIfExists=True)

# FUNCIONES AUXILIARES
def create_dataset(name, id_repo, name_repo):
    dataset = alphacast.datasets.create(name, id_repo, name_repo)
    return dataset

def initialize_columns(df, id_dataset, date_name):
    if (len(df.columns)-2)<20:
        initialize = alphacast.datasets.dataset(id_dataset).initialize_columns(dateColumnName = date_name, entitiesColumnNames=["country"],
                                                            dateFormat= "%Y-%m-%d")
    else:
        initialize = alphacast.datasets.dataset(id_dataset).initialize_columns(dateColumnName = date_name, entitiesColumnNames=[df.columns[1:].tolist()],
                                                            dateFormat= "%Y-%m-%d")
    return initialize

def send_info(df, id_dataset):
    send = alphacast.datasets.dataset(id_dataset).upload_data_from_df (df, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True, uploadIndex=False)
    return send

#creacion_dataset = create_dataset("Actividad - Argentina - IPCVA - Producción - Serie de Indicadores", 1505, "Prueba")


# ARMADO DEL DATASET
anio_actual = datetime.today().year
#url2 = "http://www.ipcva.com.ar/estadisticas/vista_serie_indicadores_xls.php?desde=1958&hasta={}".format(anio_actual)
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly

url_base = "http://www.ipcva.com.ar/vertext.php?id=964"
url_parte2 = "?desde=1958&hasta={}".format(anio_actual)

r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)

url2 = parser_base.xpath("//div[@class='span3'][2]//li/a/@href")[2].replace(".php", "_xls.php") + url_parte2
r2 = requests.get(url2, headers = encabezados)
html_doc2 = r2.text
parser2  =  html.fromstring (html_doc2)
df_list2 = pd.read_html(html_doc2)

def nan_(x):
    x = x
    if x == -0.01:
        x = np.nan
    return x
df_indicadores = df_list2[1].iloc[1:,]
df_indicadores.columns = df_indicadores.iloc[0]
df_indicadores = df_indicadores.iloc[1:]
df_indicadores["Faena"] = df_indicadores["Faena"].apply(lambda x: x.replace(".", ""))
df_indicadores["Faena"] = df_indicadores["Faena"].astype(int)/100
df_indicadores["Produccion (TN)"] = df_indicadores["Produccion (TN)"].apply(lambda x: x.replace(".", ""))
df_indicadores["Produccion (TN)"] = df_indicadores["Produccion (TN)"].astype(int)/100
df_indicadores["KG Gancho"] = df_indicadores["KG Gancho"].apply(lambda x: x.replace(".", ""))
df_indicadores["KG Gancho"] = df_indicadores["KG Gancho"].astype(int)/100
df_indicadores["Consumo Kg/hab/año"] = df_indicadores["Consumo Kg/hab/año"].apply(lambda x: x.replace(".", ""))
df_indicadores["Consumo Kg/hab/año"] = df_indicadores["Consumo Kg/hab/año"].astype(int)/100
df_indicadores["country"] = "Argentina"
df_indicadores["Año"] = df_indicadores["Año"].apply(lambda x: pd.to_datetime(x + "-" + "01"))

for col in df_indicadores.columns.tolist():
#     if ((col != "country") & (col != "Fecha")):
    if (~(col.startswith("country")) & (col != "Año") & (col != "Existencias")):
        df_indicadores[col] = df_indicadores[col].astype(str)
        df_indicadores[col] = df_indicadores[col].apply(lambda x: x.replace("S/D", "-1")).astype(float)        
        df_indicadores[col] = df_indicadores[col].apply(lambda x: nan_(x))
    elif (col == "Existencias"):
        df_indicadores[col] = df_indicadores[col].astype(str)
        df_indicadores[col] = df_indicadores[col].apply(lambda x: x.replace("S/D", "-1")).apply(lambda x: x.replace(".", "")).astype(float)
        df_indicadores[col] = df_indicadores[col].apply(lambda x: x/100)
        df_indicadores[col] = df_indicadores[col].apply(lambda x: nan_(x))


# ENVIO DE INFO HACIA ALPHACAST
#inicializacion = initialize_columns(df_indicadores, xxxx, "Periodo")
#envio = send_info(df_indicadores, xxxx)