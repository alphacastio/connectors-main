#Librerias

import pandas as pd
import requests
from lxml import html
from datetime import datetime
pd.options.display.float_format = '{:.2f}'.format
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly

#from alphacast import Alphacast
# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)

#OBTENCION URL BASE
url_base = "https://www.mae.com.ar/mercado/estadisticas/volumen-negociado"
r_base = requests.get(url_base, headers = encabezados)
html_doc_base = r_base.text
parser_base  =  html.fromstring (html_doc_base)

### Financial - Argentina - MAE - Fixed-Income repo volume - Monthly

## CREACION E INICIALIZACION DATASET
# alphacast.datasets.create("Financial - Argentina - MAE - Fixed-Income repo volume - Monthly", 1505, "Prueba")
# alphacast.datasets.dataset(9112).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["country"],
#                                                      dateFormat= "%Y-%m-%d")

## ARMADO DATASET
url2 = parser_base.xpath("//td/a/@href")[1]
r2 = requests.get(url2, headers = encabezados)
html_doc2 = r2.text
df_list2 = pd.read_html(html_doc2)
renta_fija_vol_repo = df_list2[0]
renta_fija_vol_repo = pd.DataFrame(renta_fija_vol_repo.set_index("Month").stack().reset_index())
renta_fija_vol_repo["Año"] = renta_fija_vol_repo[renta_fija_vol_repo.columns.tolist()[1]].apply(lambda x: x[5:-1].strip())
renta_fija_vol_repo["Month"] = pd.to_datetime(renta_fija_vol_repo["Año"] + "-" + renta_fija_vol_repo["Month"])
renta_fija_vol_repo =renta_fija_vol_repo.iloc[:,[0,2]]
renta_fija_vol_repo.columns = ["Date", "Volume"]
renta_fija_vol_repo["country"] = "Argentina"
renta_fija_vol_repo = renta_fija_vol_repo.sort_values("Date")

## ENVIO DE DATOS HACIA ALPHACAST
# alphacast.datasets.dataset(9112).upload_data_from_df (renta_fija_vol_repo, deleteMissingFromDB = False, 
#                                                       onConflictUpdateDB = True, uploadIndex=False)