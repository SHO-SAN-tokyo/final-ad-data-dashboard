
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from google.cloud import bigquery

# ------------------------------------------------------------
# 0. ページ設定
# ------------------------------------------------------------
st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧩 SHO‑SAN market")
st.subheader("📊 カテゴリ × 都道府県 キャンペーン達成率モニター")

# ------------------------------------------------------------
# 1. データ読み込み
# ------------------------------------------------------------
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\n", "
")
client = bigquery.Client.from_service_account_info(cred)

@st.cache_data(show_spinner=False)
def load_data():
    df = client.query("""
        SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Market_Monthly_Evaluated_View`
    """).to_dataframe()
    return df

df = load_data()

# ------------------------------------------------------------
# 2. 前処理
# ------------------------------------------------------------
df["配信月"] = pd.to_datetime(df["配信月"] + "-01", errors="coerce")
df["達成率"] = df["達成率"].astype(float)
指標 = st.selectbox("📌 表示指標を選択", ["CPA", "CVR", "CTR", "CPC", "CPM"])

# ------------------------------------------------------------
# 3. フィルター
# ------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    cat = st.selectbox("📁 カテゴリ", ["すべて"] + sorted(df["カテゴリ"].dropna().unique()))
with col2:
    pref = st.selectbox("🗾 都道府県", ["すべて"] + sorted(df["都道府県"].dropna().unique()))
with col3:
    obj = st.selectbox("🎯 広告目的", ["すべて"] + sorted(df["広告目的"].dropna().unique()))

if cat != "すべて":
    df = df[df["カテゴリ"] == cat]
if pref != "すべて":
    df = df[df["都道府県"] == pref]
if obj != "すべて":
    df = df[df["広告目的"] == obj]

# ------------------------------------------------------------
# 4. 表示テーブル
# ------------------------------------------------------------
st.markdown("### 📋 達成率一覧")
df_table = df[["配信月", "都道府県", "カテゴリ", "広告目的", "CampaignName",
               指標, "目標値", "達成率", "評価"]].sort_values(["配信月", "都道府県", "CampaignName"])
st.dataframe(df_table, use_container_width=True, hide_index=True)

# ------------------------------------------------------------
# 5. 月別推移グラフ
# ------------------------------------------------------------
st.markdown("### 📈 月別推移グラフ")
df_plot = (
    df.groupby("配信月")
      .agg(実績値=(指標, "mean"), 目標値=("目標値", "mean"))
      .reset_index()
)
fig = px.line(df_plot, x="配信月", y=["実績値", "目標値"], markers=True)
fig.update_layout(yaxis_title=指標, xaxis_title="配信月", height=400)
st.plotly_chart(fig, use_container_width=True)
