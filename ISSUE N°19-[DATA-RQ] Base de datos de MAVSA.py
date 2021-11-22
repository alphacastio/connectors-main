#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Instalacion de liberias en caso de no tenerlas
#get_ipython().system('pip install wget')
#get_ipython().system('pip install lxml')
#get_ipython().system('pip install requests')
#get_ipython().system('pip install os')
#get_ipython().system('pip install pandas')


# In[3]:


#Importacion de librerias:

from zipfile import ZipFile
import pandas as pd
import os
import requests
from lxml import html
import wget


# In[4]:


# URL para la obtencion de datos y encabezados para que no me baneen el requests

url_mav = "https://www.mav-sa.com.ar/research/cpd-pagares-y-fce/mensual/"
encabezados  = {"user-agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}


# In[5]:


# Establecer conexion con la URL y automatizar mediante web scraping la lista de formatos ZIPs que se encuentran en la página.

def conexion(url_mav, encabezados):    
    encabezados = encabezados
    response = requests.get(url_mav,  headers = encabezados, verify=False)
    parser  =  html.fromstring (response.text)
    pdf_path = parser.xpath ("//div[@class='col-md-6 col-sm-6']//a/@href")
    list_final_path = [path for path in pdf_path if path.endswith('.zip')]
    
    return list_final_path


# In[6]:


# Llamado a la funcion conexion() con sus parametros para obtener los ZIPs que deben utilizarse para generar el dataset final

list_final_path = conexion(url_mav, encabezados)


# In[7]:


# Descargar cada ZIP obtenido en la funcion anterior mediante wget en una carpeta de interes, en mi caso la ruta donde se descargan
# estos archivos es en C:\Users\ezequiel\APIs\Alphacast-Datos Economicos\ISSUES\Anexos\ pero debe seleccionarse una de interes
# de quien ejecute el código. Asi mismo a cada ZIP se le agrega un nombre de cada mes. 

for i in list_final_path:
    wget.download(i, r'C:\Users\ezequiel\APIs\Alphacast-Datos Economicos\ISSUES\Anexos\{}'.format(i[-14:]))


# In[8]:


# Parseo de cada ZIP descargado anteriormente en la direccion elegida, descomprimiendo los mismos y obteniendo solo los archivos
# que empiecen con Anexo IV. Luego con pandas read_excel() se guarda cada excel en una lista de dataframes.

def lectura_archivos_comprimidos():
#     lis_dir = os.listdir(r'C:\Users\ezequiel\APIs\Alphacast-Datos Economicos\ISSUES\Anexos')
    path = "C:/Users/ezequiel/APIs/Alphacast-Datos Economicos/ISSUES/Anexos/"
    list_dataframes = []    
    for i in os.listdir(r'C:\Users\ezequiel\APIs\Alphacast-Datos Economicos\ISSUES\Anexos'):    
        with ZipFile(path + i , 'r') as obj_zip:    
            FileNames = obj_zip.namelist()
            #FileNames = FileNames[-2]
            FileNames = [i for i in FileNames if i.startswith('Anexos/Anexo IV')][0]
            obj_zip.extract(FileNames)
    b = []
    a = os.listdir(r"C:\Users\ezequiel\APIs\Alphacast-Datos Economicos\ISSUES\Anexos")
    for i in a:
        if i.endswith(".xlsx"):
            b.append(i)
    for i in b:            
        df = pd.read_excel(r'C:\Users\ezequiel\APIs\Alphacast-Datos Economicos\ISSUES\Anexos\{}'.format(i))
        list_dataframes.append(df)
    return list_dataframes


# In[9]:


# Se unen los todos los dataframes que se generaron en la lista anterior mediante pandas.concat uno abajo del otro(axis=0).

def inner_dataframes():
    list_dataframes = lectura_archivos_comprimidos()
    df_final = pd.DataFrame()
    for df in list_dataframes:
        df_final = pd.concat([df_final, df],join='outer', axis=0)
    return df_final


# In[10]:


# Se obtiene el df final ordenado por fecha y hora y por último se filtran solo las columnas indicadas en el ISSUE.

def df_final_transforms():
    df_final = inner_dataframes()
    df_final['Fec.Con.'] = pd.to_datetime(df_final['Fec.Con.'])
    df_final = df_final.sort_values(['Fec.Con.','Hora'], ascending = True, ignore_index = True)
    df_final = df_final[['Fec.Con.', 'Reg.', 'Operatoria', 'Mon.', 'Vencimiento', 'Warrantera/Deudor Cedido/Libr./SGR', 'Tasa', 'Monto Nominal', 'Monto Liquid.']]
    return df_final


# In[11]:


#Obtención del dataframe final lista para subir a ALphacast

df_final = df_final_transforms()


# In[13]:


df_final

