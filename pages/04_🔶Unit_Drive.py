import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="🔶 Unit Drive", layout="wide")
st.title("🔶 Unitごとのスコアカード")

# BigQuery 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# テーブル情報
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
fact_table = f"{project_id}.{dataset}.Final_Ad_Data"
unit_table = f"{project_id}.{dataset}.UnitMapping"

@st.cache_data(ttl=600)
def load_data():
    fact_df = client.query(f"SELECT * FROM `{fact_table}`").to_dataframe()
    unit_df = client.query(f"SELECT * FROM `{unit_table}`").to_dataframe()
    return fact_df, unit_df

df, unit_df = load_data()
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
unit_df = unit_df.rename(columns={"所属": "Unit"})

# ------------------- 📅 日付フィルター -------------------
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
date_range = st.date_input("📅 日付で絞り込み", (min_date, max_date), min_value=min_date, max_value=max_date)
start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# 最新レコード（CV数・予算・フィー）をキャンペーンID単位で抽出
latest = df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()][
    ["CampaignId", "コンバージョン数", "予算", "フィー", "担当者", "フロント"]
]

latest = latest.merge(unit_df[["担当者", "Unit"]], on="担当者", how="left")

for col in latest.columns:
    if not pd.api.types.is_datetime64_any_dtype(latest[col]):
        latest[col] = latest[col].replace([float("inf"), -float("inf")], 0).fillna(0)

# 消化金額（Cost）は合計
cost_df = df.groupby("CampaignId")[["Cost"]].sum().reset_index()
campaign_df = latest.merge(cost_df, on="CampaignId", how="left")

# Unitごとの集計
unit_summary = campaign_df.groupby("Unit").agg(
    平均CPA=("Cost", lambda x: round(x.sum() / campaign_df.loc[x.index, "コンバージョン数"].replace(0, pd.NA).fillna(1).sum())),
    キャンペーン数=("CampaignId", "nunique"),
    予算=("予算", "sum"),
    フィー=("フィー", "sum"),
    消化金額=("Cost", "sum"),
    CV数=("コンバージョン数", "sum")
).reset_index()

unit_colors = ["#c9d8d2", "#dce2dc", "#e6dada", "#f5e6cc", "#e6cccc", "#d1e0e0", "#f5f5f5", "#dedede"]
unit_color_map = {unit: unit_colors[i % len(unit_colors)] for i, unit in enumerate(sorted(unit_summary["Unit"].unique()))}

# ユニットごとのスコアカード
st.markdown("#### 🧩 Unitごとのスコアカード")
unit_cols = st.columns(3)
for i, row in unit_summary.sort_values("Unit").iterrows():
    with unit_cols[i % 3]:
        st.markdown(f"""
        <div style="background-color:{unit_color_map[row['Unit']]}; padding:1.5rem; border-radius:1rem; text-align:center; margin-bottom:1rem;">
            <h3 style="margin-bottom:0.5rem;">{row['Unit']}</h3>
            <h2 style="margin:0;">¥{int(row['平均CPA']):,}</h2>
            <div style="font-size:0.9rem; margin-top:1rem;">
                キャンペーン数: {int(row['キャンペーン数'])}<br>
                予算: ¥{int(row['予算']):,}<br>
                消化金額: ¥{int(row['消化金額']):,}<br>
                フィー: ¥{int(row['フィー']):,}<br>
                CV: {int(row['CV数'])}
            </div>
        </div>
        """, unsafe_allow_html=True)

# ------------------- 🔍 担当者条件フィルター -------------------
st.markdown("#### 🧑‍💼 担当者ごとのスコアカード")

col1, col2, col3 = st.columns(3)
担当者一覧 = sorted(campaign_df["担当者"].dropna().unique())
フロント一覧 = sorted(campaign_df["フロント"].dropna().unique())
ユニット一覧 = sorted(campaign_df["Unit"].dropna().unique())

with col1:
    selected_unit = st.selectbox("Unit", ["すべて"] + ユニット一覧)
with col2:
    selected_tantousha = st.selectbox("担当者", ["すべて"] + 担当者一覧)
with col3:
    selected_front = st.selectbox("フロント", ["すべて"] + フロント一覧)

filtered_df = campaign_df.copy()
if selected_unit != "すべて":
    filtered_df = filtered_df[filtered_df["Unit"] == selected_unit]
if selected_tantousha != "すべて":
    filtered_df = filtered_df[filtered_df["担当者"] == selected_tantousha]
if selected_front != "すべて":
    filtered_df = filtered_df[filtered_df["フロント"] == selected_front]

# 担当者ごとの集計
person_summary = filtered_df.groupby("担当者").agg(
    平均CPA=("Cost", lambda x: round(x.sum() / filtered_df.loc[x.index, "コンバージョン数"].replace(0, pd.NA).fillna(1).sum())),
    キャンペーン数=("CampaignId", "nunique"),
    予算=("予算", "sum"),
    フィー=("フィー", "sum"),
    消化金額=("Cost", "sum"),
    CV数=("コンバージョン数", "sum")
).reset_index()

person_color_map = {name: unit_colors[i % len(unit_colors)] for i, name in enumerate(person_summary["担当者"])}

person_cols = st.columns(4)
for i, row in person_summary.iterrows():
    with person_cols[i % 4]:
        st.markdown(f"""
        <div style="background-color:{person_color_map[row['担当者']]}; padding:1.2rem; border-radius:1rem; text-align:center; margin-bottom:1rem;">
            <h4 style="margin-bottom:0.5rem;">{row['担当者']}</h4>
            <h3 style="margin:0;">¥{int(row['平均CPA']):,}</h3>
            <div style="font-size:0.85rem; margin-top:1rem;">
                キャンペーン数: {int(row['キャンペーン数'])}<br>
                予算: ¥{int(row['予算']):,}<br>
                消化金額: ¥{int(row['消化金額']):,}<br>
                フィー: ¥{int(row['フィー']):,}<br>
                CV: {int(row['CV数'])}
            </div>
        </div>
        """, unsafe_allow_html=True)
