import streamlit as st
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="Unit Drive", layout="wide")
st.title("🚗 Unit Drive")

# --- BigQuery 認証 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# --- データ読み込み ---
@st.cache_data(ttl=60)
def load_data():
    ad_df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    unit_map = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.UnitMapping`").to_dataframe()
    return ad_df, unit_map

ad_df, unit_map = load_data()

# --- 日付フィルター ---
ad_df["Date"] = pd.to_datetime(ad_df["Date"], errors="coerce")
min_date = ad_df["Date"].min().date()
max_date = ad_df["Date"].max().date()
date_range = st.date_input("📅 日付で絞り込む", (min_date, max_date), min_value=min_date, max_value=max_date)
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    ad_df = ad_df[(ad_df["Date"] >= start_date) & (ad_df["Date"] <= end_date)]

# --- Unit/担当者/フロントの紐づけ ---
ad_df = ad_df.merge(unit_map, how="left", on="担当者")

# --- 最新の1行だけ使う値 ---
latest = ad_df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()]

# --- キャンペーン単位に集計 ---
agg = ad_df.groupby("CampaignId").agg({
    "Cost": "sum"
}).reset_index()

latest_cols = ["CampaignId", "コンバージョン数", "予算", "フィー"]
latest_values = latest[latest_cols].copy()
for col in ["コンバージョン数", "予算", "フィー"]:
    latest_values[col] = pd.to_numeric(latest_values[col], errors="coerce").fillna(0)

campaign_df = agg.merge(latest_values, on="CampaignId", how="left")
campaign_df = campaign_df.merge(latest[["CampaignId", "担当者", "フロント", "Unit"]], on="CampaignId", how="left")

campaign_df["CPA"] = campaign_df["Cost"] / campaign_df["コンバージョン数"].replace(0, pd.NA)

# --- Unit単位に集計 ---
unit_summary = campaign_df.groupby("Unit").agg(
    CPA_mean=("CPA", "mean"),
    campaign_count=("CampaignId", "nunique"),
    budget_total=("予算", "sum"),
    cost_total=("Cost", "sum"),
    fee_total=("フィー", "sum"),
    cv_total=("コンバージョン数", "sum")
).reset_index()

unit_summary = unit_summary.sort_values("Unit")

# --- 表示 ---
st.markdown("""<style>.card-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1.5rem; }</style>""", unsafe_allow_html=True)

st.markdown("<div class='card-grid'>", unsafe_allow_html=True)

color_palette = ["#f2f2f2", "#e8eaf6", "#e3f2fd", "#e0f7fa", "#e8f5e9", "#f9fbe7"]
for idx, row in unit_summary.iterrows():
    bg = color_palette[idx % len(color_palette)]
    st.markdown(f"""
    <div style='background:{bg}; padding:1.5rem; border-radius:1rem; text-align:center; box-shadow:0 2px 4px rgba(0,0,0,0.05);'>
        <div style='font-size:1.5rem; font-weight:600;'>{row['Unit']}</div>
        <div style='font-size:1.3rem; font-weight:500; margin:0.5rem 0;'>CPA: ¥{row['CPA_mean']:,.0f}</div>
        <div style='font-size:0.9rem; line-height:1.4;'>📊 キャンペーン数: {row['campaign_count']}<br>
        💰 予算: ¥{row['budget_total']:,.0f}<br>
        🔥 消化金額: ¥{row['cost_total']:,.0f}<br>
        🎯 CV数: {row['cv_total']}<br>
        💼 フィー: ¥{row['fee_total']:,.0f}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)
