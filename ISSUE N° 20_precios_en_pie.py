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
#                                            , privacy="Public", returnIdIfExists=True)

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

### Actividad - Argentina - IPCVA - Precios Internacionales - Precios en Pie
#creacion_dataset = alphacast.datasets.create("Actividad - Argentina - IPCVA - Precios Internacionales - Precios en Pie", 1505, "Prueba")

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
    if x == -0.01:
        x = np.nan
    return x

def conecting_data(url, encabezados):
    r = requests.get(url, headers = encabezados)
    html_doc = r.text
    parser  =  html.fromstring (html_doc)
    df_list = pd.read_html(html_doc)
    return df_list

anio_actual = datetime.today().year
mes_actual = datetime.today().month
dia_actual = datetime.today().day
#url9 = "http://www.ipcva.com.ar/estadisticas/vista_precios_xls.php?id=1&desde=2004-01-01&hasta={}-{}-{}&categorias=1,2,3,4,5,6,7&paises=1,2,3,4,5,6,7,8,203".format(anio_actual, mes_actual, dia_actual)
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly
url_base = "http://www.ipcva.com.ar/vertext.php?id=964"
url_parte2 = "&desde=2004-01-01&hasta={}-{}-{}&categorias=1,2,3,4,5,6,7&paises=1,2,3,4,5,6,7,8,203".format(anio_actual, mes_actual, dia_actual)
r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)
url9 = parser_base.xpath("//div[@class='span3'][4]//li/a/@href")[0].replace(".php", "_xls.php") + url_parte2
con_precios_pie = conecting_data(url9, encabezados)
con_precios_pie = con_precios_pie[1]
con_precios_pie = con_precios_pie.iloc[1:,]
con_precios_pie.columns = con_precios_pie.iloc[0]
con_precios_pie = con_precios_pie.iloc[1:]
col_dataset_ppie = con_precios_pie.columns.to_list()
for i, col in enumerate(col_dataset_ppie):
    col = col + "-" + str(con_precios_pie.iloc[1,i])
    col_dataset_ppie[i] = col
con_precios_pie.iloc[1] = col_dataset_ppie
con_precios_pie.columns = con_precios_pie.iloc[0]
con_precios_pie = con_precios_pie.set_index("Fecha")
columns_trans = con_precios_pie.columns.unique().tolist()
df_append = []
for pais in columns_trans:
    df = con_precios_pie.loc[:,pais]
    df = pd.DataFrame(df)
    df.columns = df.iloc[1]
    df = df.iloc[2:]    
    df["country"] = pais
    #df = df.rename(columns={pais:"country"})    
    df_append.append(df)
df_final = pd.DataFrame()
for df in df_append:
    df_final = pd.concat([df_final, df], axis=1)
df_final = df_final.reset_index()
col_dataset_df_final = df_final.columns.to_list()
for i, col in enumerate(col_dataset_df_final):
    if (col_dataset_df_final.count(col)>1):
        col = col + "." + str(col_dataset_df_final.count(col)-1)
        col_dataset_df_final[i] = col
    else:
        col = col
        col_dataset_df_final[i] = col
df_final.columns = col_dataset_df_final
df_final = df_final.dropna()
df_final[df_final.columns[0]] = df_final[df_final.columns[0]].apply(lambda x: x.split("-")[1].capitalize() + "-" + x.split("-")[0] + "-" + x.split("-")[2]).apply(lambda x: tunning_date(x)).apply(lambda x: pd.to_datetime(x))
df_final = df_final.rename(columns={df_final.columns[0]:"Fecha"})
df_final = df_final.sort_values("Fecha")
for col in df_final.columns.tolist():
    if (~(col.startswith("country")) & (col != "Fecha")):
        df_final[col] = df_final[col].apply(lambda x: x.replace("S/D", "-1")).astype(float)
        df_final[col] = df_final[col].apply(lambda x: x/100)
        df_final[col] = df_final[col].apply(lambda x: nan_(x))

#inicializacion = initialize_columns(df_final, id_dataset, "Fecha")
#envio = send_info(df_final, id_dataset)