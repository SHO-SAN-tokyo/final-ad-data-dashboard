import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="SHO-SANマーケット", layout="wide")
st.title("🧩 SHO-SAN 広告市場")

# BigQuery 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# テーブル情報
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "UnitMapping"
full_table = f"{project_id}.{dataset}.{table}"
