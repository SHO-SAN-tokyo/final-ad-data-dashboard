import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

st.set_page_config(page_title="🔶Unit Drive", layout="wide")
st.title("🔶 Unit Drive")

# --- 認証 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# --- データ読み込み ---
@st.cache_data(ttl=600)
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    mapping = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.UnitMapping`").to_dataframe()
    return df, mapping

df, mapping_df = load_data()

# --- 日付変換とフィルター ---
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
date_min = df["Date"].min().date()
date_max = df["Date"].max().date()
selected_range = st.date_input("📅 表示期間", (date_min, date_max), min_value=date_min, max_value=date_max)

if isinstance(selected_range, (list, tuple)) and len(selected_range) == 2:
    start, end = pd.to_datetime(selected_range[0]), pd.to_datetime(selected_range[1])
    df = df[(df["Date"] >= start) & (df["Date"] <= end)]

# --- 最新CV、予算、フィー抽出 ---
latest = df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()]

# キャンペーンIDごとの集計
grouped = df.groupby("CampaignId").agg({
    "Cost": "sum"
}).rename(columns={"Cost": "消化金額"})

latest = latest.merge(grouped, on="CampaignId", how="left")
latest["コンバージョン数"] = pd.to_numeric(latest["コンバージョン数"], errors="coerce").fillna(0)
latest["予算"] = pd.to_numeric(latest["予算"], errors="coerce").fillna(0)
latest["フィー"] = pd.to_numeric(latest["フィー"], errors="coerce").fillna(0)

# CPA 計算
latest["CPA"] = latest["消化金額"] / latest["コンバージョン数"]
latest = latest.replace([float("inf"), -float("inf")], 0).fillna(0)

# --- Unit Mapping のマージ ---
unit_df = mapping_df.rename(columns={"所属": "Unit"})
latest = latest.merge(unit_df, on="担当者", how="left")

# --- Unitごとのスコアカード ---
st.subheader("🧩 Unitごとのスコアカード")
unit_color_map = {
    "UnitA": "#d0e0d8",
    "UnitB": "#f5e6cc",
    "UnitC": "#f2e4dc",
    "UnitD": "#dce5f2",
    "UnitE": "#e8dff5"
}

unit_summary = latest.groupby("Unit").agg({
    "CampaignId": "nunique",
    "予算": "sum",
    "消化金額": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum"
}).reset_index()
unit_summary["CPA"] = unit_summary["消化金額"] / unit_summary["コンバージョン数"]
unit_summary = unit_summary.fillna(0).replace([float("inf"), -float("inf")], 0)
unit_summary = unit_summary.sort_values("Unit")

cols = st.columns(3)
for i, (_, row) in enumerate(unit_summary.iterrows()):
    color = unit_color_map.get(row["Unit"], "#eeeeee")
    with cols[i % 3]:
        st.markdown(f"""
            <div style='background-color:{color}; padding:1.5rem; border-radius:1rem; text-align:center; margin-bottom:1rem;'>
                <h3 style='margin-bottom:0.5rem;'>{row['Unit']}</h3>
                <div style='font-size:1.5rem; font-weight:bold;'>¥{row['CPA']:,.0f}</div>
                <div style='font-size:0.9rem; margin-top:1rem;'>キャンペーン数: {row['CampaignId']}<br>
                予算: ¥{row['予算']:,.0f}<br>
                消化金額: ¥{row['消化金額']:,.0f}<br>
                フィー: ¥{row['フィー']:,.0f}<br>
                CV: {int(row['コンバージョン数'])}</div>
            </div>
        """, unsafe_allow_html=True)

# --- フィルター（Unitごとのあとに移動） ---
st.subheader("🧑‍💼 担当者ごとのスコアカード")
col1, col2, col3 = st.columns(3)
担当者_options = sorted(latest["担当者"].dropna().unique())
フロント_options = sorted(latest["フロント"].dropna().unique())
ユニット_options = sorted(latest["Unit"].dropna().unique())

with col1:
    selected_tantou = st.multiselect("担当者", 担当者_options)
with col2:
    selected_front = st.multiselect("フロント", フロント_options)
with col3:
    selected_unit = st.multiselect("ユニット", ユニット_options)

filtered = latest.copy()
if selected_tantou:
    filtered = filtered[filtered["担当者"].isin(selected_tantou)]
if selected_front:
    filtered = filtered[filtered["フロント"].isin(selected_front)]
if selected_unit:
    filtered = filtered[filtered["Unit"].isin(selected_unit)]

# --- 担当者ごとのスコアカード ---
tantou_summary = filtered.groupby("担当者").agg({
    "CampaignId": "nunique",
    "予算": "sum",
    "消化金額": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum",
    "Unit": "first"
}).reset_index()
tantou_summary["CPA"] = tantou_summary["消化金額"] / tantou_summary["コンバージョン数"]
tantou_summary = tantou_summary.fillna(0).replace([float("inf"), -float("inf")], 0)

cols = st.columns(4)
for i, (_, row) in enumerate(tantou_summary.iterrows()):
    unit = row["Unit"]
    color = unit_color_map.get(unit, "#eeeeee")
    with cols[i % 4]:
        st.markdown(f"""
            <div style='background-color:{color}; padding:1.5rem; border-radius:1rem; text-align:center; margin-bottom:1rem;'>
                <h4 style='margin-bottom:0.5rem;'>{row['担当者']}</h4>
                <div style='font-size:1.5rem; font-weight:bold;'>¥{row['CPA']:,.0f}</div>
                <div style='font-size:0.9rem; margin-top:1rem;'>キャンペーン数: {row['CampaignId']}<br>
                予算: ¥{row['予算']:,.0f}<br>
                消化金額: ¥{row['消化金額']:,.0f}<br>
                フィー: ¥{row['フィー']:,.0f}<br>
                CV: {int(row['コンバージョン数'])}</div>
            </div>
        """, unsafe_allow_html=True)
