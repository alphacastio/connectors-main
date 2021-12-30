# Librerias
import pandas as pd
import requests
from lxml import html
from alphacast import Alphacast
pd.options.display.float_format = '{:.2f}'.format

## CREACION REPO Y DATASETS EN ALPHACAST

# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)

# respositorio = alphacast.repository.create("Uruguay Macro Basics", repo_description="Uruguay Macro Basics", slug="Uruguay"
#                                              , privacy="Public", returnIdIfExists=True)

# alphacast.datasets.create("Activity - Uruguay - BCU - Physical Volume Index", 46, "Uruguay Macro Basics")

# alphacast.datasets.dataset({id_dataset}).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Country"],
#                                                       dateFormat= "%Y-%m-%d")

# alphacast.datasets.dataset({id_dataset}).upload_data_from_df (dataset, deleteMissingFromDB = False, 
#                                                       onConflictUpdateDB = True, uploadIndex=False)

## ARMADO FUNCION PARA CORREGIR FECHAS

def transform_date(fecha):
    fecha = fecha    
    if fecha[0:3]=="I 2":
        fecha = fecha.replace("I ","01-").replace("*", "")
    elif fecha[0:3]=="II ":
        fecha = fecha.replace("II ","04-").replace("*", "")
    elif fecha[0:3]=="III":
        fecha = fecha.replace("III ","07-").replace("*", "")
    else:
        fecha = fecha.replace("IV ","10-").replace("*", "") 
        
    return fecha


# DATAPOINT
 
url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx"
encabezados  = {"user-agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}
r = requests.get(url, headers = encabezados, verify=False)
html_doc = r.text
parser  =  html.fromstring (html_doc)

## LIMPIEZA Y TRANSFORMACION DATASETS

link_datasets = parser.xpath ("//td/a[contains(@href, 'Desestacionalizado')]/@href")[0].replace(" ", "%20")
dataset = pd.read_excel(link_datasets, skiprows=7, nrows=1).dropna(axis=1).T.reset_index()
dataset = dataset.rename(columns= {dataset.columns[0]:"Date", dataset.columns[1]:"Producto Interno Bruto"}).iloc[1:]
dataset["Date"] = dataset.Date.apply(lambda x: transform_date(x))
dataset["Date"] = pd.to_datetime(dataset["Date"])
dataset["Producto Interno Bruto"] = dataset["Producto Interno Bruto"].astype(float)
dataset["Country"] = "Uruguay"