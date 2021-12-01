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

# ### Actividad - Argentina - IPCVA - Mercado Doméstico - Precios al Consumidor

#creacion_dataset = create_dataset(Actividad - Argentina - IPCVA - Mercado Doméstico - Precios al Consumidor", 1505, "Prueba")

# ARMADO DATASET
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

def conecting_data(url, encabezados):
    r = requests.get(url, headers = encabezados)
    html_doc = r.text
    parser  =  html.fromstring (html_doc)
    df_list = pd.read_html(html_doc)
    return df_list

def build_df(df):
    df = df.iloc[1:,]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df["Período"] = pd.to_datetime(df["Período"].apply(lambda x: tunning_date(x)), format="%m-%y")
    df = df.sort_values("Período", ignore_index=True)
    for col in df.columns.tolist():
        if (~(col.startswith("country")) & (col != "Período")):
            df[col] = df[col].astype(str)
            df[col] = df[col].apply(lambda x: x.replace("$", "").strip()).apply(lambda x: x.replace(".", "").strip()).apply(lambda x: x.replace(",", ".").strip())
            df[col] = df[col].apply(lambda x: x.replace("S/D", "-1")).astype(float)        
            df[col] = df[col].apply(lambda x: nan_(x))
    df = df.set_index("Período")
    return df

anio_actual = datetime.today().year
mes_actual = datetime.today().month
#url6 = "http://www.ipcva.com.ar/estadisticas/vista_precios_consumidor_xls.php?productos=10,70,15,71,72,12,86,73,14,74&desde=1990-01-01&hasta={}-{}-01".format(anio_actual, mes_actual)
#url7 = "http://www.ipcva.com.ar/estadisticas/vista_precios_consumidor_xls.php?productos=16,75,76,77,87,13,78,11,79,80&desde=1990-01-01&hasta={}-{}-01".format(anio_actual, mes_actual)
#url8 = "http://www.ipcva.com.ar/estadisticas/vista_precios_consumidor_xls.php?productos=81,82,17,83,88,84,89,85&desde=1990-01-01&hasta={}-{}-01".format(anio_actual, mes_actual)
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly
url_base = "http://www.ipcva.com.ar/vertext.php?id=964"
url_parte2_6 = "productos=10,70,15,71,72,12,86,73,14,74&desde=1990-01-01&hasta={}-{}-01".format(anio_actual, mes_actual)
url_parte2_7 = "productos=16,75,76,77,87,13,78,11,79,80&desde=1990-01-01&hasta={}-{}-01".format(anio_actual, mes_actual)
url_parte2_8 = "productos=81,82,17,83,88,84,89,85&desde=1990-01-01&hasta={}-{}-01".format(anio_actual, mes_actual)
r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)
url6 = parser_base.xpath("//div[@class='span3'][3]//li/a/@href")[3].replace(".php", "_xls.php") + url_parte2_6
url7 = parser_base.xpath("//div[@class='span3'][3]//li/a/@href")[3].replace(".php", "_xls.php") + url_parte2_7
url8 = parser_base.xpath("//div[@class='span3'][3]//li/a/@href")[3].replace(".php", "_xls.php") + url_parte2_8

con_precios_consum1 =  conecting_data(url6, encabezados)
con_precios_consum2 = conecting_data(url7, encabezados)
con_precios_consum3 = conecting_data(url8, encabezados)
df_precios_consum1 = build_df(con_precios_consum1[0])
df_precios_consum2 = build_df(con_precios_consum2[0])
df_precios_consum3 = build_df(con_precios_consum3[0])
df_precios_consum = pd.concat([df_precios_consum1, df_precios_consum2, df_precios_consum3], axis=1).reset_index()
df_precios_consum["country"] = "Argentina"

#inicializacion = initialize_columns(df_precios_consum, id_dataset, "Periodo")
#envio = send_info(df_precios_consum, id_dataset)