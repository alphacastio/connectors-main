import pandas as pd
import requests
from bs4 import BeautifulSoup
pd.options.display.float_format = '{:.3f}'.format

# from alphacast import Alphacast
# with open ('ApiKey.csv', 'r') as API_key:
#     API_key = API_key.readline().strip()
# alphacast = Alphacast(API_key)

# respositorio = alphacast.repository.create("USA Macro Basics", repo_description="USA Macro Basics", slug="USA"
#                                              , privacy="Public", returnIdIfExists=True)

# alphacast.datasets.create("Activity - USA - IHS Markit - Monthly GDP Estimate", 47, "USA Macro Basics")

# alphacast.datasets.dataset({id_dataset}).initialize_columns(dateColumnName = "Date", entitiesColumnNames=["Country"],
#                                                       dateFormat= "%Y-%m-%d")

# alphacast.datasets.dataset({id_dataset}).upload_data_from_df (dataset, deleteMissingFromDB = False, 
#                                                       onConflictUpdateDB = True, uploadIndex=False)


url_base = "https://ihsmarkit.com/products/us-monthly-gdp-index.html"
encabezados  = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"} #change the version of the browser accordingly

response = requests.get(url_base, headers = encabezados)
html_doc_base = response.text
soup = BeautifulSoup(html_doc_base, "html.parser")
url_excel = soup.find(class_="grid-3 cta-container article-intro").a.get("href")

df_GDP = pd.read_excel(url_excel, sheet_name = "Data")
df_GDP.insert(1, "Country", "United States")
df_GDP = df_GDP.rename(columns={df_GDP.columns[0]:"Date"})
df_GDP["Date"] = pd.to_datetime(df_GDP["Date"])







