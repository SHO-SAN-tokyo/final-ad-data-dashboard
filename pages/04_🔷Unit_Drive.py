import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np

st.set_page_config(page_title="Unit Drive", layout="wide")
st.title("🔷 Unit Drive")

st.subheader("📊 広告TM パフォーマンス")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ取得（VIEW）
@st.cache_data(show_spinner="データ取得中…")
def load_data():
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Unit_Drive_Ready_View").to_dataframe()
    return df

df = load_data()

# 日付型の変換（配信開始日/終了日はDATE型なのでこのままでOK）

# 📅 配信月フィルター
month_options = sorted(df["配信月"].dropna().unique())
selected_month = st.selectbox("📅 配信月", ["すべて"] + month_options)
if selected_month != "すべて":
    df = df[df["配信月"] == selected_month]

# 所属・担当者・フロント・雇用形態フィルター
latest = df.copy()
numeric_cols = latest.select_dtypes(include=["number"]).columns
latest[numeric_cols] = latest[numeric_cols].replace([np.inf, -np.inf], 0).fillna(0)
latest = latest[latest["所属"].notna()]
latest = latest[latest["所属"].apply(lambda x: isinstance(x, str))]

unit_options = latest["所属"].dropna().unique()
person_options = latest["担当者"].dropna().astype(str).unique()
front_options = latest["フロント"].dropna().astype(str).unique()
employment_options = latest["雇用形態"].dropna().astype(str).unique()

f1, f2, f3, f4 = st.columns([2, 2, 2, 2])
with f1:
    unit_filter = st.selectbox("🏷️ Unit", ["すべて"] + sorted(unit_options))
with f2:
    person_filter = st.selectbox("👤 担当者", ["すべて"] + sorted(person_options))
with f3:
    front_filter = st.selectbox("👤 フロント", ["すべて"] + sorted(front_options))
with f4:
    employment_filter = st.selectbox("🏢 雇用形態", ["すべて"] + sorted(employment_options), key="employment_type")

# --- フィルター選択状況を1行で表示 ---
st.markdown(f"""
<div style='padding: 0.8rem 0 1.2rem 0; font-size: 0.9rem; border-radius: 0.5rem;'>
    📅 配信月: <b>{selected_month}</b>　
    |　🏷️Unit: <b>{unit_filter}</b>　
    |　👤担当者: <b>{person_filter}</b>　
    |　👤フロント: <b>{front_filter}</b>　
    |　🏢雇用形態: <b>{employment_filter}</b>
</div>
""", unsafe_allow_html=True)

# --- フィルター適用 ---
df_filtered = latest.copy()
if unit_filter != "すべて":
    df_filtered = df_filtered[df_filtered["所属"] == unit_filter]
if person_filter != "すべて":
    df_filtered = df_filtered[df_filtered["担当者"] == person_filter]
if front_filter != "すべて":
    df_filtered = df_filtered[df_filtered["フロント"] == front_filter]
if employment_filter != "すべて":
    df_filtered = df_filtered[df_filtered["雇用形態"] == employment_filter]

# 集計用定義（CPA 0割り対策も込み）
def safe_cpa(cost, cv):
    return cost / cv if cv > 0 else np.nan

# Unitごとのサマリー
unit_summary = df_filtered.groupby("所属", dropna=False).agg({
    "CampaignId": "nunique",
    "予算": "sum",
    "消化金額": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum"
}).reset_index()
unit_summary["CPA"] = unit_summary.apply(
    lambda row: safe_cpa(row["消化金額"], row["コンバージョン数"]), axis=1
)
unit_summary = unit_summary.sort_values("所属")

# --- Unit別色マップ
unit_colors = ["#c0e4eb", "#cbebb5", "#ffdda6"]
unit_color_map = {unit: unit_colors[i % len(unit_colors)] for i, unit in enumerate(unit_summary["所属"].unique())}

# --- Unitカード ---
st.write("#### 🍋🍋‍🟩 Unitごとのスコア 🍒🍏")
unit_cols = st.columns(3)
for idx, row in unit_summary.iterrows():
    with unit_cols[idx % 3]:
        st.markdown(f"""
        <div style='background-color: {unit_color_map.get(row["所属"], "#f0f0f0")}; padding: 1.2rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
            <div style='font-size: 1.3rem; font-weight: bold; text-align: center;'>{row['所属']}</div>
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

# --- 担当者別カード ---
st.write("#### 👨‍💼 担当者ごとのスコア")
person_summary = df_filtered.groupby("担当者", dropna=False).agg({
    "CampaignId": "nunique",
    "予算": "sum",
    "消化金額": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum"
}).reset_index()
person_summary["CPA"] = person_summary.apply(
    lambda row: safe_cpa(row["消化金額"], row["コンバージョン数"]), axis=1
)
person_summary = person_summary.sort_values("担当者")
person_summary = person_summary.merge(
    latest[["担当者", "所属"]].drop_duplicates(), on="担当者", how="left"
)
person_cols = st.columns(5)
for idx, row in person_summary.iterrows():
    color = unit_color_map.get(row.get("所属"), "#f0f0f0")
    with person_cols[idx % 5]:
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

# 達成率スコアカード
if "達成状況" in df_filtered.columns:
    st.write("### 👨‍💼 担当者ごとの達成率")
    person_agg = df_filtered.groupby("担当者", dropna=False).agg(
        campaign_count=("CampaignId", "nunique"),
        達成件数=("達成状況", lambda x: (x == "達成").sum())
    ).reset_index()
    person_agg["達成率"] = person_agg["達成件数"] / person_agg["campaign_count"]
    person_agg = person_agg.sort_values("達成率", ascending=False)
    person_cols = st.columns(5)
    for idx, row in person_agg.iterrows():
        with person_cols[idx % 5]:
            st.markdown(f"""
            <div style='background-color: #f0f5eb; padding: 1rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
                <h5>{row["担当者"]}</h5>
                <div style='font-size: 1.2rem; font-weight: bold;'>{row["達成率"]:.0%}</div>
                <div style='font-size: 0.9rem;'>
                    達成数: {int(row["達成件数"])} / {int(row["campaign_count"])}
                </div>
            </div>
            """, unsafe_allow_html=True)

# ▼ キャンペーン一覧（必要なカラム全て追加＆整形）
st.write("#### 📋 配信キャンペーン一覧（最大1000件）")
columns_to_show = [
    "配信月","キャンペーン名","担当者","所属","フロント","雇用形態",
    "予算","フィー","クライアント名","消化金額","canvaURL",
    "カテゴリ","媒体","広告目的",
    "コンバージョン数","CPA","CVR","CTR","CPC","CPM",
    "CPA_KPI_評価","個別CPA_達成","CTR_KPI_評価","CPC_KPI_評価","CPM_KPI_評価"
]
columns_to_show = [col for col in columns_to_show if col in df_filtered.columns]
styled_table = df_filtered[columns_to_show].head(1000).style.format({
    "予算": "¥{:,.0f}",
    "フィー": "¥{:,.0f}",
    "消化金額": "¥{:,.0f}",
    "コンバージョン数": "{:,.0f}",
    "CPA": "¥{:,.0f}",
    "CVR": "{:.1%}",
    "CTR": "{:.1%}",
    "CPC": "¥{:,.0f}",
    "CPM": "¥{:,.0f}"
})
st.dataframe(styled_table, use_container_width=True)

# --- 達成キャンペーン一覧 ---
if "達成状況" in df_filtered.columns:
    st.write("### 👍 達成キャンペーン一覧")
    achieved = df_filtered[df_filtered["達成状況"] == "達成"]
    st.dataframe(
        achieved[[
            "配信月", "キャンペーン名", "担当者", "所属",
            "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"
        ]].style.format({
            "CPA": "¥{:,.0f}",
            "目標CPA": "¥{:,.0f}"
        }),
        use_container_width=True
    )

    # --- 未達成キャンペーン一覧 ---
    st.write("### 💤 未達成キャンペーン一覧")
    missed = df_filtered[df_filtered["達成状況"] == "未達成"]
    st.dataframe(
        missed[[
            "配信月", "キャンペーン名", "担当者", "所属",
            "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"
        ]].style.format({
            "CPA": "¥{:,.0f}",
            "目標CPA": "¥{:,.0f}"
        }),
        use_container_width=True
    )
