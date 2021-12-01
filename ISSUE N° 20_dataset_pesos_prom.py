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

#creacion_dataset = create_dataset("Actividad - Argentina - IPCVA - Producción - Pesos Promedio", 1505, "Prueba")

def tunning_date(x):
    x = x
    if x.startswith("Ene"):
        x = x.replace("Ene", "01")
    elif x.startswith("Feb"):
        x = x.replace("Feb", "02")
    elif x.startswith("Mar"):
        x = x.replace("Mar", "03")
    elif x.startswith("Abr"):
        x = x.replace("Abr", "04")
    elif x.startswith("May"):
        x = x.replace("May", "05")
    elif x.startswith("Jun"):
        x = x.replace("Jun", "06")
    elif x.startswith("Jul"):
        x = x.replace("Jul", "07")
    elif x.startswith("Ago"):
        x = x.replace("Ago", "08")
    elif x.startswith("Sep"):
        x = x.replace("Sep", "09")
    elif x.startswith("Oct"):
        x = x.replace("Oct", "10")
    elif x.startswith("Nov"):
        x = x.replace("Nov", "11")
    else:
        x =x.replace("Dic", "12")
    return x

def nan_(x):
    x = x
    if x == -1:
        x = np.nan
    return x

anio_actual = datetime.today().year
mes_actual = datetime.today().month
#url4 = "http://www.ipcva.com.ar/estadisticas/vista_pesos_promedio_xls.php?desde=1999-01-01&hasta={}-{}-01&categorias=1,2,3,4,5,6,7".format(anio_actual, mes_actual)
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly
url_base = "http://www.ipcva.com.ar/vertext.php?id=964"
url_parte2 = "?desde=1999-01-01&hasta={}-{}-01&categorias=1,2,3,4,5,6,7".format(anio_actual, mes_actual)
r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)
url4 = parser_base.xpath("//div[@class='span3'][2]//li/a/@href")[4].replace(".php", "_xls.php") + url_parte2
r4 = requests.get(url4, headers = encabezados)
html_doc4 = r4.text
parser4  =  html.fromstring (html_doc4)
df_list4 = pd.read_html(html_doc4)

df_pesos_prom = df_list4[0].iloc[1:,]
df_pesos_prom.columns = df_pesos_prom.iloc[0]
df_pesos_prom = df_pesos_prom.iloc[1:]
df_pesos_prom["Período"] = pd.to_datetime(df_pesos_prom["Período"].apply(lambda x: tunning_date(x)), format="%m-%y")
df_pesos_prom = df_pesos_prom.sort_values("Período", ignore_index=True)
df_pesos_prom["country"] = "Argentina"
for col in df_pesos_prom.columns.tolist():
    if (~(col.startswith("country")) & (col != "Período")):
        df_pesos_prom[col] = df_pesos_prom[col].astype(str)
        df_pesos_prom[col] = df_pesos_prom[col].apply(lambda x: x.replace("S/D", "-1")).astype(float)        
        df_pesos_prom[col] = df_pesos_prom[col].apply(lambda x: nan_(x))
        df_pesos_prom[col] = df_pesos_prom[col].apply(lambda x: x/100)

#inicializacion = initialize_columns(df_pesos_prom, id_dataset, "Periodo")
#envio = send_info(df_pesos_prom, id_dataset)