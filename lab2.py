import boto3
import requests
import io
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


url = "https://bank.gov.ua/NBU_Exchange/exchange_site"
params = {
    "start": "20210101",
    "end": "20211231",
    "valcode": "usd",
    "sort": "exchangedate",
    "order": "desc",
    "json": ""
}

response = requests.get(url, params=params)
data = response.json()

df = pd.DataFrame(data)
df.to_csv("exchange_rates_2021.csv", index=False)

s3 = boto3.client("s3", region_name="eu-north-1")
bucket_name = "bobkabucketlab4"

with open("exchange_rates_2021.csv", "rb") as f:
    s3.upload_fileobj(f, bucket_name, "exchange_rates_2021.csv")

url = "https://bank.gov.ua/NBU_Exchange/exchange_site"
params = {
    "start": "20210101",
    "end": "20211231",
    "valcode": "eur",
    "sort": "exchangedate",
    "order": "desc",
    "json": ""
}

response = requests.get(url, params=params)
data = response.json()

df = pd.DataFrame(data)
df.to_csv("exchange_rates_2021_eur.csv", index=False)

with open("exchange_rates_2021_eur.csv", "rb") as f:
    s3.upload_fileobj(f, bucket_name, "exchange_rates_2021_eur.csv")

obj = s3.get_object(Bucket=bucket_name, Key="exchange_rates_2021.csv")
df_usd = pd.read_csv(io.BytesIO(obj["Body"].read()))

obj = s3.get_object(Bucket=bucket_name, Key="exchange_rates_2021_eur.csv")
df_eur = pd.read_csv(io.BytesIO(obj["Body"].read()))

df_usd['exchangedate'] = pd.to_datetime(df_usd['exchangedate'], format='%d.%m.%Y')
df_eur['exchangedate'] = pd.to_datetime(df_eur['exchangedate'], format='%d.%m.%Y')

fig, ax = plt.subplots()
ax.plot(df_usd["exchangedate"], df_usd["rate"], label='USD')
ax.plot(df_eur["exchangedate"], df_eur["rate"], label='EUR')
ax.set_title("Exchange Rates in 2021")
ax.set_xlabel("Date")
ax.set_ylabel("Exchange Rate")
ax.legend()

ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
ax.tick_params(axis='x', rotation=45)

filename = "exchange_rates.png"
temp_file = io.BytesIO()
plt.savefig(temp_file, format='png')
s3.put_object(Body=temp_file.getvalue(), Bucket=bucket_name, Key=filename)