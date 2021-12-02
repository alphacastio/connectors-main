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

### Financiero - Argentina - MAE - Renta Fija Volumen compra / venta - Mensual

## CREACION E INICIALIZACION DATASET
# alphacast.datasets.create("Financial - Argentina - MAE - Fixed-Income purchase/sale volume - Monthly", 1505, "Prueba")
# alphacast.datasets.dataset(9111).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["country"],
#                                                      dateFormat= "%Y-%m-%d")

## ARMADO DATASET
url = parser_base.xpath("//td/a/@href")[0]
r = requests.get(url, headers = encabezados)
html_doc = r.text
df_list = pd.read_html(html_doc)
renta_fija_vol_compra = df_list[0]
renta_fija_vol_compra = pd.DataFrame(renta_fija_vol_compra.set_index("Month").stack().reset_index())
renta_fija_vol_compra["Año"] = renta_fija_vol_compra[renta_fija_vol_compra.columns.tolist()[1]].apply(lambda x: x[5:-1].strip())
renta_fija_vol_compra["Month"] = pd.to_datetime(renta_fija_vol_compra["Año"] + "-" + renta_fija_vol_compra["Month"])
renta_fija_vol_compra =renta_fija_vol_compra.iloc[:,[0,2]]
renta_fija_vol_compra.columns = ["Date", "Volume"]
renta_fija_vol_compra["country"] = "Argentina"
renta_fija_vol_compra = renta_fija_vol_compra.sort_values("Date")

## ENVIO DE DATOS HACIA ALPHACAST
# alphacast.datasets.dataset(9111).upload_data_from_df (renta_fija_vol_compra, deleteMissingFromDB = False, 
#                                                       onConflictUpdateDB = True, uploadIndex=False)
