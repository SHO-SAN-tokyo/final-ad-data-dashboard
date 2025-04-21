# Unit Driveページ
import streamlit as st
import pandas as pd
from google.cloud import bigquery

# ページ設定
st.set_page_config(page_title="Unit Drive", layout="wide")
st.title("🚗 Unit Drive")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# テーブル情報
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
main_table = f"{project_id}.{dataset}.Final_Ad_Data"
mapping_table = f"{project_id}.{dataset}.UnitMapping"

# データ取得
@st.cache_data(ttl=60)
def load_data():
    df = client.query(f"SELECT * FROM `{main_table}`").to_dataframe()
    unit_map = client.query(f"SELECT * FROM `{mapping_table}`").to_dataframe()
    return df, unit_map

df, unit_map = load_data()

# 日付変換
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# 日付フィルタ
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
selected_date = st.date_input("📅 日付範囲", (min_date, max_date), min_value=min_date, max_value=max_date)
if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
    start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
    df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# Unit 組み合わせ
merged = pd.merge(df, unit_map, on="担当者", how="left")
merged = merged.dropna(subset=["所属"])  # Unitが決まっているものみ

# フィルター
st.markdown("### 🎛 条件で絞り込む")
col1, col2, col3 = st.columns(3)

unit_list = sorted(merged["所属"].dropna().unique())
tantousha_list = sorted(merged["担当者"].dropna().unique())
front_list = sorted(merged["Front"].dropna().unique()) if "Front" in merged.columns else []

with col1:
    selected_units = st.multiselect("所属ユニット", unit_list, default=unit_list)
with col2:
    selected_tantousha = st.multiselect("担当者", tantousha_list)
with col3:
    selected_front = st.multiselect("フロント", front_list)

# フィルタ適用
filtered = merged[merged["所属"].isin(selected_units)]
if selected_tantousha:
    filtered = filtered[filtered["担当者"].isin(selected_tantousha)]
if selected_front:
    filtered = filtered[filtered["Front"].isin(selected_front)]

# 統計
summary = filtered.groupby("所属").agg({
    "CampaignId": "count",
    "予算": "sum",
    "Cost": "sum",
    "フィー": "sum",
    "CPA": "mean"
}).rename(columns={
    "CampaignId": "キャンペーン数",
    "予算": "予算",
    "Cost": "消化金額",
    "フィー": "フィー",
    "CPA": "平均CPA"
}).reset_index().sort_values("所属")

# カラー
color_palette = ["#9db4c0", "#a5c0b5", "#d9cab3", "#c8b6a6", "#a9c5cb", "#e1dad2"]

# スコアカード
st.markdown("### 🧾 Unit別 スコアカード")
cols = st.columns(3)

for i, (idx, row) in enumerate(summary.iterrows()):
    with cols[i % 3]:
        st.markdown(f"""
        <div style='background:{color_palette[i % len(color_palette)]}; padding:1.2rem; border-radius:1rem; text-align:center;'>
            <h3 style='margin-bottom:0.5rem;'>{row['所属']}</h3>
            <h2 style='margin:0.5rem 0;'>{int(row['平均CPA']):,} 円</h2>
            <div style='font-size:0.9rem; margin-top:0.5rem;'>
                キャンペーン数：{row['キャンペーン数']}<br>
                予算：{int(row['予算']):,} 円<br>
                消化金額：{int(row['消化金額']):,} 円<br>
                フィー：{int(row['フィー']):,} 円
            </div>
        </div>
        """, unsafe_allow_html=True)
