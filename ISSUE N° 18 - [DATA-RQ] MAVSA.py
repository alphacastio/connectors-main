#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importacion de librerias
import pandas as pd
import requests
from lxml import html


# In[2]:


pd.options.display.float_format = '{:.2f}'.format


# In[3]:


from alphacast import Alphacast
with open ('ApiKey.csv', 'r') as API_key:
    API_key = API_key.readline().strip()
alphacast = Alphacast(API_key)


# # Creacion Repositorio en Alphacast

# In[ ]:


# respositorio = alphacast.repository.create("Financial-Argentina-MAVSA", repo_description="ASSET-SEGMENT", slug="MAVSA"
#                                             , privacy="Public", returnIdIfExists=True)


# # Creacion de los dataset en Alphacast

# In[ ]:


#alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - Avalado- Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - Garant.- Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - No Garant. - Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - CPD ECHEQ - Warant - Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - PAGARÉ - Avalado - Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - PAGARÉ - No Garant. - Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - FCE - No Garant. - Diario.", 1428, "Financial-Argentina-MAVSA")
# alphacast.datasets.create("Financiero - Argentina - MAVSA - Caucion Pesos - Diario.", 1428, "Financial-Argentina-MAVSA")


# # Inicializacion columnas para cada uno de los 8 datasets creado
# 

# In[4]:


#hacer para cada dataset creado:

# alphacast.datasets.dataset(8606).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8598).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8599).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8600).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8601).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8602).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8603).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")
# alphacast.datasets.dataset(8604).initialize_columns(dateColumnName = "date", entitiesColumnNames=["Plazo"],
#                                                      dateFormat= "%Y-%m-%d")


# In[5]:


# URL y encabezados
url2 = "https://www.mav-sa.com.ar/#"
encabezados  = {"user-agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}


# In[6]:


# Conexión con URL y parseo de html
r = requests.get(url2, headers = encabezados, verify=False)
html_doc = r.text
parser  =  html.fromstring (html_doc)
df_list = pd.read_html(html_doc)


# In[7]:


# Obtencion dinamica de fechas para cada dataset mediante web scraping
fecha_CPD_ECHEQ = parser.xpath ("//div[@id='collapseCPD-ECHEQ']/h3/span/text()")[0][-9:].strip()
fecha_PAGARE = parser.xpath ("//div[@id='collapsePAGARE']/h3/span/text()")[0][-9:].strip()
fecha_FCE = parser.xpath ("//div[@id='collapseFCE']/h3/span/text()")[0][-9:].strip()
fecha_Caucion_Pesos = parser.xpath ("//div[@id='collapsecaucionpesos']/h3/span/text()")[0][-9:].strip()


# In[8]:


#Obtencion de los dataframes con los datos brutos
CPD_ECHEQ_Avalado_Diario = df_list[0]
CPD_ECHEQ_Garant_Diario = df_list[1]
CPD_ECHEQ_No_Garant_Diario = df_list[2]
CPD_ECHEQ_Warant_Diario = df_list[3]
PAGARE_Avalado_Diario = df_list[4]
PAGARE_No_Garant_Diario = df_list[5]
FCE_No_Garant_Diario = df_list[6]
Caucion_Pesos_Diario = df_list[7]


# In[9]:


# Función de limpieza y trasnformación del de cada dataset
def transform_data(df, fecha):
    try:
        df["Monto Nom."] = df["Monto Nom."].apply(lambda x: x/100)
        df["Monto Liq."] = df["Monto Liq."].apply(lambda x: x/100)
        df["date"] = pd.to_datetime(fecha)
        df.insert(0,"date", df.pop('date'))
    except:
        df["date"] = pd.to_datetime(fecha)
        df.insert(0,"date", df.pop('date'))
    return df    


# In[10]:


# Obtencion de los datasets finales
CPD_ECHEQ_Avalado_Diario = transform_data(CPD_ECHEQ_Avalado_Diario, fecha_CPD_ECHEQ)
CPD_ECHEQ_Garant_Diario = transform_data(CPD_ECHEQ_Garant_Diario, fecha_CPD_ECHEQ)
CPD_ECHEQ_No_Garant_Diario = transform_data(CPD_ECHEQ_No_Garant_Diario, fecha_CPD_ECHEQ)
CPD_ECHEQ_Warant_Diario = transform_data(CPD_ECHEQ_Warant_Diario, fecha_CPD_ECHEQ)
PAGARE_Avalado_Diario = transform_data(PAGARE_Avalado_Diario, fecha_PAGARE)
PAGARE_No_Garant_Diario = transform_data(PAGARE_No_Garant_Diario, fecha_PAGARE)
FCE_No_Garant_Diario = transform_data(FCE_No_Garant_Diario, fecha_FCE)
Caucion_Pesos_Diario = transform_data(Caucion_Pesos_Diario, fecha_Caucion_Pesos)


# # Envio de la informacion a cada uno de los futuros datasets

# In[12]:


# para cada dataset con su correspondiente id

alphacast.datasets.dataset(8606).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True, uploadIndex=False)
alphacast.datasets.dataset(8598).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)
alphacast.datasets.dataset(8599).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)
alphacast.datasets.dataset(8600).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)
alphacast.datasets.dataset(8601).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)
alphacast.datasets.dataset(8602).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)
alphacast.datasets.dataset(8603).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)
alphacast.datasets.dataset(8604).upload_data_from_df (CPD_ECHEQ_Avalado_Diario, deleteMissingFromDB = False, 
                                                      onConflictUpdateDB = True)


# In[ ]:




