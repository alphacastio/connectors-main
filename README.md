# Alphacast's Open-Source Connectors

This is the main repository from where we manage the work behind Alphacast, a collaboration platform for creating, sharing and monetizing decision-making data and analysis. In the Alphacast's Open-Source Connectors you can find thousands of hours of work of our team and the community.

Our database grows on a daily basis but we will be happy if you want to help us in our endevour.

## What is the Alphacast's Open-Source Connectors?

1. This Connectors are the script behind Alphacast verified datasets. We review and validate the code to confirm that they do what they say they do. The code is then run routinely (hourly, daily, weekly) to keep the data updated on our Certified Datasets. 
2. We have begun the process of opening our Connectors. Explore the repositories to find examples, ideas and inspiration! All the code and data created in this project will be open and free! You can also use the codes in this Connectors to create you own datasets and repos with them.
3. Anyone can upload, share and monetize their data in Alphacast but please, read the [Terms & Conditions](https://www.alphacast.io/terms). Don't share or upload data that you do not own or that you do not have the right to share.
4. We support the work of our contributors. Write to hello@alphacast.io to learn more about this.

## How to contribute

If you want to contribute to this project, please follow these guidelines.

- The "issues" section is our backlog. It includes the data sources that we have identified and are planning to work on. 
- Issues are labeled by repository, source format and difficulty. Our team have estimated the aproximate work needed to solve a specific issue. It depends on your skills, but to use as a benchmark: Easy should require less than one hour of work, intermediate from one to four, difficult from 4 to 8.
- The Connectors should do three things
1. Download the data from the source, parse it, clean it and structure it, preferently on a Pandas DataFrame.
2. Create and initialize the dataset (only to be run once) in YOUR repository. Once aproved we will move it to an Alphacast Certified Dataset. 
3. Upload the data using the API or the Pyhon Library.

## General Guidelines

1. You can use our python library to interact with the API. See the documentation [here](https://alphacast-python-sdk.readthedocs.io/en/latest/reference.html#quick-start) or connect directly with the API ([Postman Doc](https://documenter.getpostman.com/view/17184186/TzzDLb94))

2. Use your API_KEY and your repository. use a .env with the variable

```
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)
```

3. Create and initialize the dataset and upload the data to confirm that everything works properly!

```
dataset_name = 'your dataset_name'
dataset = alphacast.datasets.create(dataset_name, your_repo_id)
alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')

alphacast.datasets.dataset(dataset['id']).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=False)
```

4. Submit you code with a Pull Request refering to the issue either in this Repository or in the destination Repository
