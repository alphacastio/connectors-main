#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importacion de librerias

import pandas as pd
pd.options.display.float_format = '{:.2f}'.format


# In[2]:


# URL para obtener los datos

url = "https://datos.produccion.gob.ar/dataset/bf0da6da-4cb0-4362-a925-0bdd69cf1c61/resource/f476cffb-7121-4f03-8cda-e51523c034e0/download/consumo_electrico_sectores_manufactureros_original.csv"


# In[3]:


# limpieza y generación de datos

def consumo_electrico_original(url):
    consumo_electrico_original = pd.read_csv(url)
    consumo_electrico_original["country"] = "Argentina"
    consumo_electrico_original.rename(columns={"periodo":"date"}, inplace = True)
    consumo_electrico_original["date"] = pd.to_datetime(consumo_electrico_original["date"])
    return consumo_electrico_original


# In[4]:


consumo_electrico_original = consumo_electrico_original(url)


# In[5]:


consumo_electrico_original

