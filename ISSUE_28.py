# Librerias
import pandas as pd
import requests
from lxml import html
pd.options.display.float_format = '{:.2f}'.format

## CREACION REPO Y DATASETS PRUEBA EN ALPHACAST
#from alphacast import Alphacast
# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)

# respositorio = alphacast.repository.create("Prueba_Paraguay Macro Basics", repo_description="Paraguay Macro Basics", slug="Paraguay"
#                                              , privacy="Public", returnIdIfExists=True)

# alphacast.datasets.create("Activity - Paraguay - BCP - Gross Domestic Product - Supply Approach", 1445, "Prueba_Paraguay Macro Basics")
# alphacast.datasets.create("Activity - Paraguay - BCP - Gross Domestic Product - Expenditure Approach", 1445, "Prueba_Paraguay Macro Basics")

#alphacast.datasets.dataset(8662).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["country"],
#                                                       dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8663).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["country"],
#                                                       dateFormat= "%Y-%m-%d")

## ARMADO FUNCION PARA CORREGIR FECHAS

def transform_date(fecha):
    fecha = fecha
    if len(fecha) == 6:
        if fecha[-2:]=="Q1":
            fecha = fecha.replace("Q1","-01")
        elif fecha[-2:]=="Q2":
            fecha = fecha.replace("Q2","-04")
        elif fecha[-2:]=="Q3":
            fecha = fecha.replace("Q3","-07")
        else:
            fecha = fecha.replace("Q4","-10")
    else:
        if fecha[-2:]=="1*":
            fecha = fecha.replace("Q1*","-01")
        elif fecha[-2:]=="2*":
            fecha = fecha.replace("Q2*","-04")
        elif fecha[-2:]=="3*":
            fecha = fecha.replace("Q3*","-07")
        else:
            fecha = fecha.replace("Q4*","-10")
        
    return fecha

# DATAPOINT 

url = "https://www.bcp.gov.py/indice-ipp-mensual-i371"
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly

## LIMPIEZA Y TRANSFORMACION DATASETS

def build_dataset(url, encabezados, sheet_name):
    r = requests.get(url, headers = encabezados)
    html_doc = r.text
    parser  =  html.fromstring (html_doc)
    link = ['https://www.bcp.gov.py'+'/'+link for link in parser.xpath ("//div/a/@href") if link.endswith(".xlsx")][0]
    resp = requests.get(link, headers = encabezados)
    df = pd.read_excel(resp.content, skiprows=4, sheet_name=sheet_name)
    df = df.dropna(subset=[df.columns[1]])
    df = df.loc[:,df.columns[0]:df.columns[1]+".1"].drop(df.columns[1]+".1", axis=1).dropna(how='all', axis=1)
    col_datasets = df.columns.to_list()    
    for i, column in enumerate(col_datasets):
        if ((i>1) & (column.startswith("Unnamed"))):
            col_datasets[i] = col_datasets[i-1]
    df.columns = col_datasets
    col_datasets_transf = df.columns.to_list()
    for i, col in enumerate(col_datasets_transf):
        col = col + "-" + str(df.iloc[0,i])
        col_datasets_transf[i] = col
    df.columns = col_datasets_transf
    df = df.iloc[1:]
    column1 = df.columns[0]
    df = df.rename(columns={column1:"Date"})
    df["Date"] = df.Date.apply(lambda x: transform_date(x))
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.convert_dtypes()
    df["country"] = "Paraguay"

    return df

## INSTANCIACION DE CADA DATASET

PBI_oferta = build_dataset(url, encabezados, 0)
PBI_gasto = build_dataset(url, encabezados, 1)

## ENVIO DE INFORMACION HACIA ALPHACAST

#alphacast.datasets.dataset(8662).upload_data_from_df (PBI_oferta, deleteMissingFromDB = False, 
                                                      #onConflictUpdateDB = True, uploadIndex=False)
#alphacast.datasets.dataset(8663).upload_data_from_df (PBI_gasto, deleteMissingFromDB = False, 
                                                      #onConflictUpdateDB = True, uploadIndex=False)