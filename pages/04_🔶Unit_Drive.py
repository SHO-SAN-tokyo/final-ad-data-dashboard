import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# --- ページ設定 ---
st.set_page_config(page_title="Unit Drive", layout="wide")
st.title("🚗 Unit Drive")

# --- 認証 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(info_dict)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# --- テーブル情報 ---
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
source_table = f"{project_id}.{dataset}.Final_Ad_Data"
unit_table = f"{project_id}.{dataset}.ClientSetting"

# --- データ取得 ---
@st.cache_data(ttl=60)
def load_data():
    df = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()
    unit_df = client.query(f"SELECT * FROM `{unit_table}`").to_dataframe()
    return df, unit_df

df, unit_df = load_data()

# --- 日付フィルタ ---
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
min_date, max_date = df["Date"].min().date(), df["Date"].max().date()
selected_date = st.date_input("🗓️ 日付", (min_date, max_date), min_value=min_date, max_value=max_date)
if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
    start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
    df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# --- 最新CVの抽出 ---
latest = df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()]

# --- ユニット情報統合 ---
latest = latest.merge(unit_df[["担当者", "所属"]], on="担当者", how="left")
latest.rename(columns={"所属": "Unit"}, inplace=True)

# --- 各キャンペーンの済み金額は合計 ---
cost_df = df.groupby("CampaignId")["Cost"].sum().reset_index()
latest = latest.merge(cost_df, on="CampaignId", suffixes=("_latest", ""))

# --- NaN変換 ---
latest["予算"] = pd.to_numeric(latest["予算"], errors="coerce").fillna(0)
latest["フィー"] = pd.to_numeric(latest["フィー"], errors="coerce").fillna(0)
latest["コンバージョン数"] = pd.to_numeric(latest["コンバージョン数"], errors="coerce").fillna(0)
latest["Cost"] = latest["Cost"].fillna(0)

# --- CPA計算 ---
latest["CPA"] = latest["Cost"] / latest["コンバージョン数"].replace({0: None})

# --- ユニットごとに集計 ---
summary = latest.groupby("Unit").agg(
    CPA=("CPA", "mean"),
    Campaigns=("CampaignId", "nunique"),
    予算=("予算", "sum"),
    Cost=("Cost", "sum"),
    フィー=("フィー", "sum"),
).reset_index().sort_values("Unit")

# --- 表示 ---
st.markdown("---")
st.markdown("### 🔹 Unit別スコアカード")

colors = ["#b4c5d9", "#d3d8e8", "#e4eaf4", "#c9d8c5", "#c6c9d3", "#dcdcdc"]
col_count = 3
cols = st.columns(col_count)

for i, row in summary.iterrows():
    with cols[i % col_count]:
        st.markdown(f"""
        <div style='background-color:{colors[i % len(colors)]}; border-radius:1rem; padding:1.5rem; margin:1rem 0;'>
            <div style='font-size:1.5rem; font-weight:bold; text-align:center'>{row['Unit']}</div>
            <div style='font-size:1.5rem; text-align:center;'>平均CPA<br><span style='font-size:2rem;'>{row['CPA']:,.0f}円</span></div>
            <hr style='margin:1rem 0;'>
            <div style='font-size:0.9rem; text-align:center; line-height:1.6;'>
                キャンペーン数: {row['Campaigns']}<br>
                予算: {row['予算']:,.0f}円<br>
                消化金額: {row['Cost']:,.0f}円<br>
                フィー: {row['フィー']:,.0f}円
            </div>
        </div>
        """, unsafe_allow_html=True)
