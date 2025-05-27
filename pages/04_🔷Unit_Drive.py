import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime

st.set_page_config(page_title="Unit Drive", layout="wide")
st.title("🔷 Unit Drive")

st.subheader("📊 広告TM パフォーマンス")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ取得
def load_data():
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Unit_Drive_Ready_View").to_dataframe()
    unit_df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.UnitMapping").to_dataframe()
    return df, unit_df

df, unit_df = load_data()

# 前処理
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# 📅 日付フィルター
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
date_range = st.date_input("", (min_date, max_date), min_value=min_date, max_value=max_date)
df = df[(df["Date"].dt.date >= date_range[0]) & (df["Date"].dt.date <= date_range[1])]

# 最新CV, 予算、フィー（キャンペーンごとに1件）
latest = df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()]
latest = latest[["CampaignId", "コンバージョン数", "予算", "フィー", "担当者", "フロント", "CampaignName"]]

# Unitの付与
latest = latest.merge(unit_df, on="担当者", how="left")

# CPA算出用にCost集計（キャンペーンごと）
cost_df = df.groupby("CampaignId")["Cost"].sum().reset_index().rename(columns={"Cost": "消化金額"})
latest = latest.merge(cost_df, on="CampaignId", how="left")
latest = latest.replace([np.inf, -np.inf], 0).fillna(0)

# CPA計算（CV=0は0として扱う）
latest["CPA"] = latest.apply(lambda row: row["消化金額"] / row["コンバージョン数"] if row["コンバージョン数"] > 0 else 0, axis=1)

# --- Unit集計 ---
unit_summary = latest.groupby("所属").agg({
    "CampaignId": "nunique",
    "予算": "sum",
    "消化金額": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum"
}).reset_index()
unit_summary["CPA"] = unit_summary.apply(lambda row: row["消化金額"] / row["コンバージョン数"] if row["コンバージョン数"] > 0 else 0, axis=1)
unit_summary = unit_summary.sort_values("所属")

# --- Unit別色マップ（任意の3色） ---
unit_colors = ["#c0e4eb", "#cbebb5", "#ffdda6"]
unit_color_map = {unit: unit_colors[i % len(unit_colors)] for i, unit in enumerate(unit_summary["所属"].unique())}

# --- Unitカード ---
st.write("#### 🍋‍🟩 Unitごとのスコア")
unit_cols = st.columns(3)
for idx, row in unit_summary.iterrows():
    with unit_cols[idx % 3]:
        st.markdown(f"""
        <div style='background-color: {unit_color_map[row['所属']]}; padding: 1.2rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
            <h3 style='margin-bottom: 0.3rem;'>{row['所属']}</h3>
            <div style='font-size: 1.5rem; font-weight: bold;'>¥{row['CPA']:,.0f}</div>
            <div style='font-size: 0.9rem; margin-top: 0.5rem;'>
                キャンペーン数: {int(row['CampaignId'])}<br>
                予算: ¥{int(row['予算'])}<br>
                消化金額: ¥{int(row['消化金額'])}<br>
                フィー: ¥{int(row['フィー'])}<br>
                CV: {int(row['コンバージョン数'])}
            </div>
        </div>
        """, unsafe_allow_html=True)

# 余白
st.markdown("<div style='margin-top: 3.5rem;'></div>", unsafe_allow_html=True)

# --- 担当者別フィルター ---
st.write("#### 👨‍💼 担当者ごとのスコア")
col1, col2, col3 = st.columns(3)
unit_filter = col1.selectbox("Unit", ["すべて"] + sorted(latest["所属"].dropna().unique()))
person_filter = col2.selectbox("担当者", ["すべて"] + sorted(latest["担当者"].dropna().unique()))
front_filter = col3.selectbox("フロント", ["すべて"] + sorted(latest["フロント"].dropna().unique()))

filtered_df = latest.copy()
if unit_filter != "すべて":
    filtered_df = filtered_df[filtered_df["所属"] == unit_filter]
if person_filter != "すべて":
    filtered_df = filtered_df[filtered_df["担当者"] == person_filter]
if front_filter != "すべて":
    filtered_df = filtered_df[filtered_df["フロント"] == front_filter]

# --- 担当者別スコアカード ---
person_summary = filtered_df.groupby("担当者").agg({
    "CampaignId": "nunique",
    "予算": "sum",
    "消化金額": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum"
}).reset_index()
person_summary["CPA"] = person_summary.apply(lambda row: row["消化金額"] / row["コンバージョン数"] if row["コンバージョン数"] > 0 else 0, axis=1)
person_summary = person_summary.sort_values("担当者")

# 色もユニットに連動させる
person_summary = person_summary.merge(unit_df, on="担当者", how="left")

person_cols = st.columns(4)
for idx, row in person_summary.iterrows():
    color = unit_color_map.get(row["所属"], "#f0f0f0")
    with person_cols[idx % 4]:
        st.markdown(f"""
        <div style='background-color: {color}; padding: 1.2rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
            <h4 style='margin-bottom: 0.3rem;'>{row['担当者']}</h4>
            <div style='font-size: 1.3rem; font-weight: bold;'>¥{row['CPA']:,.0f}</div>
            <div style='font-size: 0.9rem; margin-top: 0.5rem;'>
                キャンペーン数: {int(row['CampaignId'])}<br>
                予算: ¥{int(row['予算'])}<br>
                消化金額: ¥{int(row['消化金額'])}<br>
                フィー: ¥{int(row['フィー'])}<br>
                CV: {int(row['コンバージョン数'])}
            </div>
        </div>
        """, unsafe_allow_html=True)

# 余白
st.markdown("<div style='margin-top: 4.5rem;'></div>", unsafe_allow_html=True)

# --- キャンペーン一覧テーブル ---
st.write("#### 📋 配信キャンペーン")
campaign_table = filtered_df[["CampaignName", "担当者", "所属", "予算", "フィー", "消化金額", "コンバージョン数", "CPA"]]
campaign_table = campaign_table.rename(columns={"所属": "Unit"})
campaign_table = campaign_table[["CampaignName", "担当者", "Unit", "予算", "フィー", "消化金額", "コンバージョン数", "CPA"]]

st.dataframe(
    campaign_table.style.format({
        "予算": "¥{:.0f}",
        "フィー": "¥{:.0f}",
        "消化金額": "¥{:.0f}",
        "CPA": "¥{:.0f}"
    }),
    use_container_width=True
)
