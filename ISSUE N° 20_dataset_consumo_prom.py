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

# ### Actividad - Argentina - IPCVA - Mercado Doméstico - Consumo Promedio

#creacion_dataset = create_dataset("Actividad - Argentina - IPCVA - Mercado Doméstico - Consumo Promedio", 1505, "Prueba")

# ARMADO DATASET
def nan_(x):
    x = x
    if x == -1:
        x = np.nan
    return x

def tunning_date(x):
    x = x
    if x.startswith("Ene"):
        x = x.replace("Enero", "01")
    elif x.startswith("Feb"):
        x = x.replace("Febrero", "02")
    elif x.startswith("Mar"):
        x = x.replace("Marzo", "03")
    elif x.startswith("Abr"):
        x = x.replace("Abril", "04")
    elif x.startswith("May"):
        x = x.replace("Mayo", "05")
    elif x.startswith("Jun"):
        x = x.replace("Junio", "06")
    elif x.startswith("Jul"):
        x = x.replace("Julio", "07")
    elif x.startswith("Ago"):
        x = x.replace("Agosto", "08")
    elif x.startswith("Sep"):
        x = x.replace("Septiembre", "09")
    elif x.startswith("Oct"):
        x = x.replace("Octubre", "10")
    elif x.startswith("Nov"):
        x = x.replace("Noviembre", "11")
    else:
        x =x.replace("Diciembre", "12")
    return x

anio_actual = datetime.today().year
#url5 = "http://www.ipcva.com.ar/estadisticas/vista_consumos_promedio_xls.php?desde=1958&hasta={}".format(anio_actual)
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly
url_base = "http://www.ipcva.com.ar/vertext.php?id=964"
url_parte2 = "?desde=1958&hasta={}".format(anio_actual)
r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)
url5 = parser_base.xpath("//div[@class='span3'][3]//li/a/@href")[0].replace(".php", "_xls.php") + url_parte2
r5 = requests.get(url5, headers = encabezados)
html_doc5 = r5.text
parser5  =  html.fromstring (html_doc5)
df_list5 = pd.read_html(html_doc5)

df_consumo_prom = df_list5[0].iloc[1:,]
df_consumo_prom.columns = df_consumo_prom.iloc[0]
df_consumo_prom = df_consumo_prom.iloc[1:-1]
df_consumo_prom = df_consumo_prom.rename(columns={df_consumo_prom.columns[0]:"Date"})
#df_consumo_prom["Date"] = df_consumo_prom["Date"].apply(lambda x: pd.to_datetime(x + "-" + "01"))
df_consumo_prom["country"] = "Argentina"
for col in df_consumo_prom.columns.tolist():
    if (~(col.startswith("country")) & (col != "Date")):
        df_consumo_prom[col] = df_consumo_prom[col].astype(str)
        df_consumo_prom[col] = df_consumo_prom[col].apply(lambda x: x.replace("S/D", "-1")).astype(float)        
        df_consumo_prom[col] = df_consumo_prom[col].apply(lambda x: nan_(x))
        df_consumo_prom[col] = df_consumo_prom[col].apply(lambda x: x/10)
df_final = []
anio = 1958
for i in range(len(df_consumo_prom)):
    serie =  pd.DataFrame(df_consumo_prom.iloc[i,1:-2])
    serie["Date"] = anio
    serie = serie.rename(columns={serie.columns[0]:"Consumo de Carne Vacuna - Kilogramos / Habitante"})
    df_final.append(serie)
    anio += 1
df_consumo_prom_transp = pd.DataFrame()
for df in df_final:
    df_consumo_prom_transp = pd.concat([df_consumo_prom_transp,df])
df_consumo_prom_transp = df_consumo_prom_transp.reset_index()
df_consumo_prom_transp[df_consumo_prom_transp.columns[0]] = df_consumo_prom_transp[df_consumo_prom_transp.columns[0]].apply(lambda x: tunning_date(x))
df_consumo_prom_transp[df_consumo_prom_transp.columns[0]] = df_consumo_prom_transp["Date"].astype(str) + "-" + df_consumo_prom_transp[df_consumo_prom_transp.columns[0]]
df_consumo_prom_transp = df_consumo_prom_transp.rename(columns={1:"Date"})
df_consumo_prom_transp = df_consumo_prom_transp.iloc[:-1,:-1]
df_consumo_prom_transp["Date"] = pd.to_datetime(df_consumo_prom_transp["Date"])
df_consumo_prom_transp["Consumo de Carne Vacuna - Kilogramos / Habitante"] = df_consumo_prom_transp["Consumo de Carne Vacuna - Kilogramos / Habitante"].astype(float)
df_consumo_prom_transp["country"] = "Argentina"

#inicializacion = initialize_columns(df_consumo_prom_transp, id_dataset, "Date")
#envio = send_info(df_consumo_prom_transp, id_dataset)