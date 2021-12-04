#IMPORTACION DE LIBRERIAS

import pandas as pd
#import requests
from lxml import html
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import time
pd.options.display.float_format = '{:.2f}'.format
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"}

# from alphacast import Alphacast
# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)

# ARMADO URL FINAL

url = "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-2-41"
opts = Options()
opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.80 Chrome/71.0.3578.80 Safari/537.36")
opts.add_argument("--incognito")
opts.add_argument("--start-maximized")
driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
driver.get(url)
time.sleep(5)
html = driver.page_source 
soup = BeautifulSoup(html, "html.parser")
driver.close()

url_base = "https://www.indec.gob.ar"
links_xls = [link["href"] for link in soup.find_all('a', href=True) if link["href"].endswith(".xls")]
link_expo = links_xls[-2]
link_impo = links_xls[-1]
url_expo = url_base + link_expo
url_impo = url_base + link_impo

#CREACION E INICIALIZACION DATASETS EN ALPHACAST

# alphacast.datasets.create("BOP - Argentina - INDEC - Price & Quantities of External Trade_expo", 1505, "Prueba")
# alphacast.datasets.create("BOP - Argentina - INDEC - Price & Quantities of External Trade_impo", 1505, "Prueba")

# alphacast.datasets.dataset(9674).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["country"],
#                                                     dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(9675).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["country"],
#                                                     dateFormat= "%Y-%m-%d")

# FUNCION TUNNING DATE:

def tunning_date(x):
    x = x
    if x.endswith("Enero"):
        x = x.replace("Enero", "01")
    elif x.endswith("Febrero"):
        x = x.replace("Febrero", "02")
    elif x.endswith("Marzo"):
        x = x.replace("Marzo", "03")
    elif x.endswith("Abril"):
        x = x.replace("Abril", "04")
    elif x.endswith("Mayo"):
        x = x.replace("Mayo", "05")
    elif x.endswith("Junio"):
        x = x.replace("Junio", "06")
    elif x.endswith("Julio"):
        x = x.replace("Julio", "07")
    elif x.endswith("Agosto"):
        x = x.replace("Agosto", "08")
    elif x.endswith("Septiembre"):
        x = x.replace("Septiembre", "09")
    elif x.endswith("Octubre"):
        x = x.replace("Octubre", "10")
    elif x.endswith("Noviembre"):
        x = x.replace("Noviembre", "11")
    else:
        x =x.replace("Diciembre", "12")
    return x

## DATASET EXPORTACIONES

expo = pd.read_excel(url_expo)
first_column = expo.columns.tolist()[0]
expo[first_column] = expo[expo.columns.tolist()[0]].fillna(method="ffill")
expo = expo.dropna(how='all', axis=1)
expo = expo.fillna(method="ffill", axis=1)
expo = expo.dropna(how='all', axis=0)
list_1 = expo.iloc[0].values.tolist()
list_2 = expo.iloc[1].values.tolist()
list_final = []
for i in range (len(list_1)):
    l = "X" + "-" + list_1[i] + "-" + list_2[i]
    list_final.append(l)
expo = expo.iloc[3:]
expo.columns = list_final
expo["Date"] = expo[expo.columns.tolist()[0]].iloc[:,0] + "-" + expo[expo.columns.tolist()[0]].iloc[:,1]
expo["Date"] = expo["Date"].apply(lambda x: tunning_date(x)).apply(lambda x: x.replace("*", "").strip())
anio_actual = datetime.today().year
column_filter = expo.columns.tolist()[2]
expo = expo.reset_index(drop = True)
index_filter = expo[expo[column_filter]==str(anio_actual)+"*"].index[0]
expo = expo.iloc[0:index_filter,2:]
expo.insert(0, 'Date', expo.pop('Date'))
expo = expo.convert_dtypes()
expo["Date"] = pd.to_datetime(expo["Date"])
expo["country"] = "Argentina"

## DATASET IMPORTACIONES

impo = pd.read_excel(url_impo)
first_column = impo.columns.tolist()[0]
impo[first_column] = impo[impo.columns.tolist()[0]].fillna(method="ffill")
impo = impo.dropna(how='all', axis=1)
impo = impo.fillna(method="ffill", axis=1)
impo = impo.dropna(how='all', axis=0)
list_1 = impo.iloc[0].values.tolist()
list_2 = impo.iloc[1].values.tolist()
list_final = []
for i in range (len(list_1)):
    l = "M" + "-" + list_1[i] + "-" + list_2[i]
    list_final.append(l)
impo = impo.iloc[3:]
impo.columns = list_final
impo["Date"] = impo[impo.columns.tolist()[0]].iloc[:,0] + "-" + impo[impo.columns.tolist()[0]].iloc[:,1]
impo["Date"] = impo["Date"].apply(lambda x: tunning_date(x)).apply(lambda x: x.replace("*", "").strip())
anio_actual = datetime.today().year
column_filter = impo.columns.tolist()[2]
impo = impo.reset_index(drop = True)
index_filter = impo[impo[column_filter]==str(anio_actual)+"*"].index[0]
impo = impo.iloc[0:index_filter,2:]
impo.insert(0, 'Date', impo.pop('Date'))
impo = impo.convert_dtypes()
impo["Date"] = pd.to_datetime(impo["Date"])
impo["country"] = "Argentina"

# ENVIO DATASETS HACIA ALPHACAST

# alphacast.datasets.dataset(9674).upload_data_from_df (expo, deleteMissingFromDB = False, 
#                                                      onConflictUpdateDB = True, uploadIndex=False)
# alphacast.datasets.dataset(9675).upload_data_from_df (impo, deleteMissingFromDB = False, 
#                                                       onConflictUpdateDB = True, uploadIndex=False)
