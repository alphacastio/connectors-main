#Importacion de librerias
import pandas as pd
import requests
from lxml import html
from datetime import datetime
pd.options.display.float_format = '{:.2f}'.format


from alphacast import Alphacast
with open ('ApiKey.csv', 'r') as API_key:
    API_key = API_key.readline().strip()
alphacast = Alphacast(API_key)

#respositorio = alphacast.repository.create("Prueba", repo_description="Prueba", slug="Prueba"
#                                             , privacy="Public", returnIdIfExists=True)
#alphacast.datasets.create("Actividad - Argentina - IPCVA - Producción - Producción en Tn. Res c / hueso", 1505, "Prueba")


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


# ### Actividad - Argentina - IPCVA - Producción - Producción en Tn. Res c / hueso

#alphacast.datasets.create("Actividad - Argentina - IPCVA - Producción - Serie de Indicadores", 1505, "Prueba")

anio_actual = datetime.today().year
mes_actual = datetime.today().month
dia_actual = datetime.today().day
#url = "http://www.ipcva.com.ar/estadisticas/vista_produccion_xls.php?desde=1999-01-01&hasta={}-{}-{}&categorias=1,2,3,4,5,6,7,8".format(anio_actual, mes_actual, dia_actual)
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly

url_base = "http://www.ipcva.com.ar/vertext.php?id=964"
url_parte2 = "?desde=1999-01-01&hasta={}-{}-{}&categorias=1,2,3,4,5,6,7,8".format(anio_actual, mes_actual, dia_actual)

r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)
url = parser_base.xpath("//div[@class='span3'][2]//li/a/@href")[0].replace(".php", "_xls.php") +url_parte2
r = requests.get(url, headers = encabezados)
html_doc = r.text
parser  =  html.fromstring (html_doc)
df_list = pd.read_html(html_doc)
df_produccion = df_list[1].iloc[1:,]
df_produccion.columns = df_produccion.iloc[0]
df_produccion = df_produccion.iloc[1:,:11]
df_produccion["Periodo"] = pd.to_datetime(df_produccion["Periodo"].apply(lambda x: tunning_date(x)), format="%m-%y")
df_produccion = df_produccion.sort_values("Periodo", ignore_index=True)
df_produccion["country"] = "Argentina"
for col in df_produccion.columns[1:-1]:
    df_produccion[col] = df_produccion[col].astype(float)
    
#inicializacion = initialize_columns(df_produccion, 8742, "Periodo")
#envio = send_info(df_produccion, 8742)
