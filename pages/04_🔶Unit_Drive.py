import streamlit as st
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="🔶 Unit Drive", layout="wide")
st.title("🔶 Unit Drive")

# 認証とクライアント準備
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ取得
@st.cache_data(ttl=60)
def load_data():
    df = client.query("""
        SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`
    """).to_dataframe()
    unit_df = client.query("""
        SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.UnitMapping`
    """).to_dataframe()
    return df, unit_df

df, unit_df = load_data()

# 日付処理と変換
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    min_date, max_date = df["Date"].min(), df["Date"].max()
    selected_range = st.date_input("📅 日付で絞り込む", (min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(selected_range, tuple):
        df = df[(df["Date"] >= pd.to_datetime(selected_range[0])) & (df["Date"] <= pd.to_datetime(selected_range[1]))]

# 数値変換とNaN処理
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["予算"] = pd.to_numeric(df.get("予算", 0), errors="coerce").fillna(0)
df["フィー"] = pd.to_numeric(df.get("フィー", 0), errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# 最新CV（1キャンペーンあたり1件）
latest = df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()]
latest = latest[["CampaignId", "担当者", "フロント", "予算", "フィー", "コンバージョン数"]]

# ユニット追加
latest = latest.merge(unit_df, on="担当者", how="left")

# キャンペーンごとの集計
grouped_cost = df.groupby("CampaignId")["Cost"].sum().reset_index()
campaign_df = grouped_cost.merge(latest, on="CampaignId", how="left")

# CPA計算（ゼロ割回避）
campaign_df["CPA"] = campaign_df["Cost"] / campaign_df["コンバージョン数"].replace({0: pd.NA})

# ユニット別スコアカード ----------------------------
st.subheader("🧩 Unitごとのスコアカード")
unit_group = campaign_df.groupby("所属")
unit_cards = []

for unit, group in sorted(unit_group, key=lambda x: x[0]):
    cpa = group["CPA"].mean()
    total_cost = group["Cost"].sum()
    total_budget = group["予算"].sum()
    total_fee = group["フィー"].sum()
    total_cv = group["コンバージョン数"].sum()
    count = group["CampaignId"].nunique()

    unit_cards.append({
        "unit": unit,
        "cpa": cpa,
        "count": count,
        "budget": total_budget,
        "cost": total_cost,
        "fee": total_fee,
        "cv": total_cv
    })

cols = st.columns(3)
for i, data in enumerate(unit_cards):
    with cols[i % 3]:
        st.markdown(f"""
        <div style='background:#f8f9fa;padding:1.2rem;border-radius:1rem;text-align:center;margin-bottom:1rem;'>
            <h3 style='margin-bottom:0.3rem;'>{data['unit']}</h3>
            <h2 style='margin:0;'>¥{data['cpa']:,.0f}</h2>
            <p style='margin:0.3rem 0;font-size:0.9rem;'>キャンペーン数: {data['count']}<br>
            予算: ¥{data['budget']:,.0f}<br>
            消化金額: ¥{data['cost']:,.0f}<br>
            フィー: ¥{data['fee']:,.0f}<br>
            CV: {int(data['cv'])}</p>
        </div>
        """, unsafe_allow_html=True)

# 担当者別スコアカード -----------------------------
st.subheader("👤 担当者ごとのスコアカード")
tantousha_group = campaign_df.groupby("担当者")
tantousha_cards = []

for name, group in sorted(tantousha_group, key=lambda x: x[0]):
    cpa = group["CPA"].mean()
    total_cost = group["Cost"].sum()
    total_budget = group["予算"].sum()
    total_fee = group["フィー"].sum()
    total_cv = group["コンバージョン数"].sum()
    count = group["CampaignId"].nunique()

    tantousha_cards.append({
        "name": name,
        "cpa": cpa,
        "count": count,
        "budget": total_budget,
        "cost": total_cost,
        "fee": total_fee,
        "cv": total_cv
    })

cols = st.columns(4)
for i, data in enumerate(tantousha_cards):
    with cols[i % 4]:
        st.markdown(f"""
        <div style='background:#ffffff;padding:1rem;border-radius:0.8rem;text-align:center;border:1px solid #ddd;margin-bottom:1rem;'>
            <h4 style='margin-bottom:0.3rem;'>{data['name']}</h4>
            <h3 style='margin:0;'>¥{data['cpa']:,.0f}</h3>
            <p style='margin:0.3rem 0;font-size:0.85rem;'>キャンペーン数: {data['count']}<br>
            予算: ¥{data['budget']:,.0f}<br>
            消化金額: ¥{data['cost']:,.0f}<br>
            フィー: ¥{data['fee']:,.0f}<br>
            CV: {int(data['cv'])}</p>
        </div>
        """, unsafe_allow_html=True)
