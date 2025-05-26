# 02_🧩SHO-SAN_market.py
import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# --- ページ設定 ---
st.set_page_config(page_title="🧩 SHO-SAN market", layout="wide")
st.title("🧩 SHO‑SAN market")
st.markdown("#### 📊 月別 達成率モニター（カテゴリ × 広告目的）")

# --- BigQuery 認証とデータ取得 ---
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(cred)

@st.cache_data(ttl=3600)
def load_data():
    query = """
        SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Market_Monthly_Evaluated_View`
    """
    return client.query(query).to_dataframe()

df = load_data()
df["配信月"] = pd.to_datetime(df["配信月"] + "-01")

# --- フィルタ ---
col1, col2, col3 = st.columns(3)
with col1:
    cat = st.selectbox("📁 カテゴリ", ["すべて"] + sorted(df["カテゴリ"].dropna().unique()))
with col2:
    obj = st.selectbox("🎯 広告目的", ["すべて"] + sorted(df["広告目的"].dropna().unique()))
with col3:
    metric = st.selectbox("📌 指標", ["CPA", "CVR", "CTR", "CPC", "CPM"])

if cat != "すべて":
    df = df[df["カテゴリ"] == cat]
if obj != "すべて":
    df = df[df["広告目的"] == obj]

# --- 達成率テーブル ---
st.markdown("### 📋 達成状況一覧（◎○△×）")

table_cols = ["配信月", "カテゴリ", "広告目的", metric, f"{metric}_best", f"{metric}_評価", f"{metric}_達成率"]
df_table = df[table_cols].copy()
df_table["配信月"] = df_table["配信月"].dt.strftime("%Y/%m")
df_table = df_table.sort_values("配信月", ascending=False)

st.dataframe(df_table.rename(columns={
    "配信月": "配信月",
    "カテゴリ": "カテゴリ",
    "広告目的": "広告目的",
    metric: "実績",
    f"{metric}_best": "目標",
    f"{metric}_評価": "評価",
    f"{metric}_達成率": "達成率（%）"
}), use_container_width=True)

# --- グラフ ---
st.markdown("### 📈 月次推移グラフ")

df_plot = df[["配信月", metric, f"{metric}_best"]].dropna()
df_plot = df_plot.groupby("配信月", as_index=False).mean(numeric_only=True)

fig = px.line(df_plot, x="配信月", y=[metric, f"{metric}_best"],
              labels={"value": "値", "配信月": "配信月", "variable": "指標"},
              title=f"{metric}の月別推移")
fig.update_traces(mode="lines+markers")
fig.update_layout(xaxis_title=None, yaxis_title=metric)
st.plotly_chart(fig, use_container_width=True)
