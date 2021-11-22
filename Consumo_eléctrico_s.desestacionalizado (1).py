#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importacion de librerias

import pandas as pd
pd.options.display.float_format = '{:.2f}'.format


# In[2]:


# URL para obtener los datos

url = "https://datos.produccion.gob.ar/dataset/bf0da6da-4cb0-4362-a925-0bdd69cf1c61/resource/3e342577-a2c3-414c-8290-39656c325c62/download/consumo_electrico_sectores_manufactureros_desestacionalizado.csv"


# In[3]:


# limpieza y generación de datos

def consumo_electrico_desestacionalizado(url):
    consumo_electrico_desestacionalizado = pd.read_csv(url)
    consumo_electrico_desestacionalizado["country"] = "Argentina"
    consumo_electrico_desestacionalizado.rename(columns={"periodo":"date"}, inplace = True)
    consumo_electrico_desestacionalizado["date"] = pd.to_datetime(consumo_electrico_desestacionalizado["date"])
    return consumo_electrico_desestacionalizado


# In[4]:


# LLamado a la función

consumo_electrico_desestacionalizado = consumo_electrico_desestacionalizado(url)


# In[5]:


consumo_electrico_desestacionalizado

