#Importacion de librerias
import pandas as pd
import requests
from lxml import html

#Acceso a Alphacast
#from alphacast import Alphacast
# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)

## CREACION REPO EN ALPHACAST
#respositorio = alphacast.repository.create("Prueba_Paraguay Macro Basics", repo_description="Paraguay Macro Basics", slug="Paraguay"
#                                              , privacy="Public", returnIdIfExists=True)

## CREACION DATASET EXPORTACIONES (IMPO SERIA IGUAL, ESTO ES A EFECTOS DEMOSTRATIVO DEl CODIGO)
#alphacast.datasets.create("BOP - Paraguay - Ministerio de Hacienda - External Trade by Main Items",
#                          1445, "Prueba_Paraguay Macro Basics")

## INICIALIZACION DATASET DE PRUEBA
#alphacast.datasets.dataset(8637).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Country"],
#                                                      dateFormat= "%Y-%m-%d")

# URL y encabezados
url = "https://www.economia.gov.py/index.php/dependencias/direccion-de-integracion/informes-1/periodicos/reporte-de-comercio-exterior-rce"
encabezados  = {"user-agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}

# ACCESO A LA WEB Y PARSEO DE HTML
r = requests.get(url, headers = encabezados)
html_doc = r.text
parser  =  html.fromstring (html_doc)

# LISTADO DE LINK CON EXCELS PARA PARSEAR
links = [link for link in parser.xpath ("//td/a/@href") if "Estadisticas_del_RCE" in link]

# FUNCION TRANSFORMACION Y OBTENCION DE DATASETS DE EXPO E IMPO
def reading_df(links):
    df_final_expo = pd.DataFrame()
    df_final_impo = pd.DataFrame()
    for link in links:
#         print(link)
        if "https://www.economia.gov.py/" in link:
            link = link
        else:
            link = "https://www.economia.gov.py{}".format(link)
        df = pd.read_excel(link, skiprows=1)
        df =df.dropna(how='all', axis=1)
        df = df.iloc[:10]        
        df = df.dropna()
        anio = int(df.iloc[0,2])
        look_up = {'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04', 'Mayo': '05',
            'Junio': '06', 'Julio': '07', 'Agosto': '08', 'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11',
            'Diciembre': '12'}
        mes_nombre = df.iloc[1,0]
        mes_dia = int(look_up[mes_nombre])
        fecha = pd.to_datetime(str(anio) + "-" + str(mes_dia))
        df = df.iloc[2:,[0,2,3,5]]
        df_expo = df.iloc[:,[0,1]]
        df_impo = df.iloc[:,[2,3]]
        df_expo.columns = ["Exports", "Amount_Millions"]
        df_impo.columns = ["Imports", "Amount_Millions"]
        df_expo["Exports"] = df_expo.Exports.apply(lambda x: "Exports-" + str(x))
        df_impo["Imports"] = df_impo.Imports.apply(lambda x: "Imports-" + str(x))
        df_expo["Date"] = fecha
        df_impo["Date"] = fecha
        df_expo["Country"] = "Paraguay"
        df_impo["Country"] = "Paraguay"
        df_expo = df_expo[["Date", "Exports", "Amount_Millions", "Country"]]
        df_impo = df_impo[["Date", "Imports", "Amount_Millions", "Country"]]
        df_impo = df_impo.convert_dtypes()
        df_expo = pd.pivot_table(df_expo, values = 'Amount_Millions', index=['Date','Country'], 
                                       columns = 'Exports').reset_index()               
        
        df_impo = pd.pivot_table(df_impo, values = 'Amount_Millions', index=['Date','Country'], 
                                       columns = 'Imports').reset_index()        
        df_final_expo = pd.concat([df_final_expo, df_expo],join='outer', axis=0)
        df_final_expo = df_final_expo.fillna(0, axis=1)
        df_final_impo = pd.concat([df_final_impo, df_impo],join='outer', axis=0)
        df_final_impo = df_final_impo.fillna(0, axis=1)
        
    return df_final_expo, df_final_impo
            
# INSTANCIACION DE LA FUNCION ANTERIOR Y OBTENCION DE LOS 2 DATASETS FINALES (EXPO e IMPO)
expo, impo = reading_df(links)

# EJEMPLO ENVIO DATASET EXPORTACIONES HACIA ALPHACAST REPO N° 1445

#alphacast.datasets.dataset(8637).upload_data_from_df (expo, deleteMissingFromDB = False, 
#                                                      onConflictUpdateDB = True, uploadIndex=False)

