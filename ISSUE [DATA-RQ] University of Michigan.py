# LIBRERIAS

import pandas as pd
import requests
from lxml import html
from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains

import time
pd.options.display.float_format = '{:.2f}'.format
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"}

#from alphacast import Alphacast
# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)


# ENVIO INFO EJEMPLO ALPHACAST-HACER PARA CADA DATASET(SE PUEDE HACER FUNCION QUE SUBA TODOS LOS DATASETS)

#alphacast.datasets.create(name_dataset_28, 1367, "University of Michigan")

#alphacast.datasets.dataset({dataset_id}).initialize_columns(dateColumnName = "date", entitiesColumnNames=[] ,dateFormat= "%Y-%m-%d")

#alphacast.datasets.dataset({dataset_id}).upload_data_from_df (dataset_28, deleteMissingFromDB = False, 
#                                                     onConflictUpdateDB = True, uploadIndex=False)

# OBTENCION Y TRANSFORMACION DE DATOS

def get_datasets():
    datasets = []
    names = []
    categories = []
    url = "https://data.sca.isr.umich.edu/data-archive/mine.php"
    opts = Options()
    opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/71.0.3578.80 Chrome/71.0.3578.80 Safari/537.36")
    opts.add_argument("--incognito")
    opts.add_argument("--start-maximized")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
    driver.get(url)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    year = "1978"
    
    for category in soup.find_all('optgroup')[1:]:
        cat = category.get("label")
        values = len(category.find_all("option"))
        for i in range(values):
            categories.append(cat)

    time.sleep(5)

    input_year = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//input[@type="simple"]')))
    actions = ActionChains(driver)
    actions.double_click(input_year).perform()

    input_year.send_keys(year)
    for i in range(1,48):
        
        name = soup.find("option", value = str(i)).text[soup.find("option", value = str(i)).text.find(":")+2:]

        select = Select(driver.find_element_by_name('table'))
        select.select_by_value(str(i))

        input_view = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//input[@type="submit"]')))
        input_view.click()

        time.sleep(3)
        html = driver.page_source
        df_list = pd.read_html(html, header=0)
        
        df= df_list[1]
        df["date"] = pd.to_datetime((df["Year"]).map(str) + "-" +(df["Month"]).map(str))
        df = df.drop(["Month", "Year"], axis=1)
        names.append(name)
        datasets.append(df)
    cat_names_datasets = [categories[i] + "-" + names[i] for i in range(len(categories))]
    dict_names_datasets = dict(zip(cat_names_datasets, datasets))
    driver.close()    
    return dict_names_datasets



list_datasets = get_datasets()
cat_names_datasets = list_datasets.keys()
cat_names_datasets = list(map(lambda x: "Activity - USA - University of Michigan - Survey of Consumers - " + x, cat_names_datasets))
datasets = list(list_datasets.values())

# NAMES DATASETS

name_dataset_1 = cat_names_datasets[0]
name_dataset_2 = cat_names_datasets[1]
name_dataset_3 = cat_names_datasets[2]
name_dataset_4 = cat_names_datasets[3]
name_dataset_5 = cat_names_datasets[4]
name_dataset_6 = cat_names_datasets[5]
name_dataset_7 = cat_names_datasets[6]
name_dataset_8 = cat_names_datasets[7]
name_dataset_9 = cat_names_datasets[8]
name_dataset_10 = cat_names_datasets[9]
name_dataset_11 = cat_names_datasets[10]
name_dataset_12 = cat_names_datasets[11]
name_dataset_13 = cat_names_datasets[12]
name_dataset_14 = cat_names_datasets[13]
name_dataset_15 = cat_names_datasets[14]
name_dataset_16 = cat_names_datasets[15]
name_dataset_17 = cat_names_datasets[16]
name_dataset_18 = cat_names_datasets[17]
name_dataset_19 = cat_names_datasets[18]
name_dataset_20 = cat_names_datasets[19]
name_dataset_21 = cat_names_datasets[20]
name_dataset_22 = cat_names_datasets[21]
name_dataset_23 = cat_names_datasets[22]
name_dataset_24 = cat_names_datasets[23]
name_dataset_25 = cat_names_datasets[24]
name_dataset_26 = cat_names_datasets[25]
name_dataset_27 = cat_names_datasets[26]
name_dataset_28 = cat_names_datasets[27]
name_dataset_29 = cat_names_datasets[28]
name_dataset_30 = cat_names_datasets[29]
name_dataset_31 = cat_names_datasets[30]
name_dataset_32 = cat_names_datasets[31]
name_dataset_33 = cat_names_datasets[32]
name_dataset_34 = cat_names_datasets[33]
name_dataset_35 = cat_names_datasets[34]
name_dataset_36 = cat_names_datasets[35]
name_dataset_37 = cat_names_datasets[36]
name_dataset_38 = cat_names_datasets[37]
name_dataset_39 = cat_names_datasets[38]
name_dataset_40 = cat_names_datasets[39]
name_dataset_41 = cat_names_datasets[40]
name_dataset_42 = cat_names_datasets[41]
name_dataset_43 = cat_names_datasets[42]
name_dataset_44 = cat_names_datasets[43]
name_dataset_45 = cat_names_datasets[44]
name_dataset_46 = cat_names_datasets[45]
name_dataset_47 = cat_names_datasets[46]

# DATASETS

dataset_1 = datasets[0]
dataset_2 = datasets[1]
dataset_3 = datasets[2]
dataset_4 = datasets[3]
dataset_5 = datasets[4]
dataset_6 = datasets[5]
dataset_7 = datasets[6]
dataset_8 = datasets[7]
dataset_9 = datasets[8]
dataset_10 = datasets[9]
dataset_11 = datasets[10]
dataset_12 = datasets[11]
dataset_13 = datasets[12]
dataset_14 = datasets[13]
dataset_15 = datasets[14]
dataset_16 = datasets[15]
dataset_17 = datasets[16]
dataset_18 = datasets[17]
dataset_19 = datasets[18]
dataset_20 = datasets[19]
dataset_21 = datasets[20]
dataset_22 = datasets[21]
dataset_23 = datasets[22]
dataset_24 = datasets[23]
dataset_25 = datasets[24]
dataset_26 = datasets[25]
dataset_27 = datasets[26]
dataset_28 = datasets[27]
dataset_29 = datasets[28]
dataset_30 = datasets[29]
dataset_31 = datasets[30]
dataset_32 = datasets[31]
dataset_33 = datasets[32]
dataset_34 = datasets[33]
dataset_35 = datasets[34]
dataset_36 = datasets[35]
dataset_37 = datasets[36]
dataset_38 = datasets[37]
dataset_39 = datasets[38]
dataset_40 = datasets[39]
dataset_41 = datasets[40]
dataset_42 = datasets[41]
dataset_43 = datasets[42]
dataset_44 = datasets[43]
dataset_45 = datasets[44]
dataset_46 = datasets[45]
dataset_47 = datasets[46]


#print("Name")
#print(name_dataset_41)
#print("-----------------------------------------------------------------------")
#print("Dataset")
#print(dataset_41)






