import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────
# ログイン認証（必要に応じて）
# ──────────────────────────────────────────────
from auth import require_login
require_login()

# --- ページ設定 ---
st.set_page_config(page_title="LP_Drive", layout="wide")
st.title("🎨 LP Score")

st.subheader("📊 LPごとの広告スコア")

# --- BigQuery認証 ---
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

# --- データ取得 ---
@st.cache_data(ttl=120)
def load_data():
    query = """
        SELECT
            `配信月`, `client_name`, `URL`, `メインカテゴリ`, `サブカテゴリ`, `キャンペーン名`, `広告目的`,
            SUM(`Cost`) AS Cost,
            SUM(`Impressions`) AS Impressions,
            SUM(`Clicks`) AS Clicks,
            SUM(CAST(`コンバージョン数` AS FLOAT64)) AS CV
        FROM SHOSAN_Ad_Tokyo.LP_Score_Ready
        GROUP BY `配信月`, `client_name`, `URL`, `メインカテゴリ`, `サブカテゴリ`, `キャンペーン名`, `広告目的`
    """
    return bq.query(query).to_dataframe()

with st.spinner("🔄 データ取得中..."):
    df = load_data()

if df.empty:
    st.warning("⚠️ データがありません")
    st.stop()

# --- データ型変換 ---
df["配信月"] = df["配信月"].astype(str)
df["client_name"] = df["client_name"].astype(str)
df["URL"] = df["URL"].astype(str)
df["メインカテゴリ"] = df["メインカテゴリ"].astype(str)
df["サブカテゴリ"] = df["サブカテゴリ"].astype(str)
df["キャンペーン名"] = df["キャンペーン名"].astype(str)
df["広告目的"] = df["広告目的"].astype(str)
df["CV"] = pd.to_numeric(df["CV"], errors="coerce").fillna(0)

# --- フィルター ---
cols = st.columns(6)
with cols[0]:
    month_opts = ["すべて"] + sorted(df["配信月"].unique())
    sel_month = st.selectbox("配信月", month_opts)
with cols[1]:
    client_opts = ["すべて"] + sorted(df["client_name"].unique())
    sel_client = st.selectbox("クライアント名", client_opts)
with cols[2]:
    url_opts = ["すべて"] + sorted(df["URL"].unique())
    sel_url = st.selectbox("URL", url_opts)
with cols[3]:
    maincat_opts = ["すべて"] + sorted(df["メインカテゴリ"].unique())
    sel_maincat = st.selectbox("メインカテゴリ", maincat_opts)
with cols[4]:
    subcat_opts = ["すべて"] + sorted(df["サブカテゴリ"].unique())
    sel_subcat = st.selectbox("サブカテゴリ", subcat_opts)
with cols[5]:
    camp_opts = ["すべて"] + sorted(df["キャンペーン名"].unique())
    sel_camp = st.selectbox("キャンペーン名", camp_opts)

# --- フィルター適用 ---
df_disp = df.copy()
if sel_month != "すべて":
    df_disp = df_disp[df_disp["配信月"] == sel_month]
if sel_client != "すべて":
    df_disp = df_disp[df_disp["client_name"] == sel_client]
if sel_url != "すべて":
    df_disp = df_disp[df_disp["URL"] == sel_url]
if sel_maincat != "すべて":
    df_disp = df_disp[df_disp["メインカテゴリ"] == sel_maincat]
if sel_subcat != "すべて":
    df_disp = df_disp[df_disp["サブカテゴリ"] == sel_subcat]
if sel_camp != "すべて":
    df_disp = df_disp[df_disp["キャンペーン名"] == sel_camp]

if df_disp.empty:
    st.warning("該当データがありません")
    st.stop()

# --- 指標計算 ---
df_disp["CPA"] = df_disp["Cost"] / df_disp["CV"].replace(0, np.nan)
df_disp["CTR"] = df_disp["Clicks"] / df_disp["Impressions"].replace(0, np.nan)
df_disp["CVR"] = df_disp["CV"] / df_disp["Clicks"].replace(0, np.nan)
df_disp["CPC"] = df_disp["Cost"] / df_disp["Clicks"].replace(0, np.nan)
df_disp["CPM"] = df_disp["Cost"] / df_disp["Impressions"].replace(0, np.nan) * 1000

# --- 書式 ---
def fmt_money(x):
    return f"{x:,.0f}円" if pd.notna(x) else "-"
def fmt_pct(x):
    return f"{x*100:.2f}%" if pd.notna(x) else "-"

df_disp["消化金額"] = df_disp["Cost"].apply(fmt_money)
df_disp["CV数"] = df_disp["CV"].astype(int)
df_disp["CPA"] = df_disp["CPA"].apply(fmt_money)
df_disp["CTR"] = df_disp["CTR"].apply(fmt_pct)
df_disp["CVR"] = df_disp["CVR"].apply(fmt_pct)
df_disp["CPC"] = df_disp["CPC"].apply(fmt_money)
df_disp["CPM"] = df_disp["CPM"].apply(fmt_money)

# --- 表示 ---
st.markdown("<h4 style='margin-top:2rem;'>📊 LP（URL）ごとの集計</h4>", unsafe_allow_html=True)

for _, row in df_disp.sort_values("Cost", ascending=False).iterrows():
    card_html = f"""
    <div style='border:1px solid #ddd; border-radius:10px; padding:16px; margin-bottom:16px; background:#fdfdfd; font-size:13px;'>
      <a href="{row['URL']}" target="_blank">🔗 {row['URL']}</a><br>
      <b>メインカテゴリ：</b>{row['メインカテゴリ']}　
      <b>サブカテゴリ：</b>{row['サブカテゴリ']}　
      <b>キャンペーン名：</b>{row['キャンペーン名']}　
      <b>広告目的：</b>{row['広告目的']}<br>
      <b>消化金額：</b>{row['消化金額']}　
      <b>CV数：</b>{row['CV数']}　
      <b>CPA：</b>{row['CPA']}　
      <b>CTR：</b>{row['CTR']}　
      <b>CVR：</b>{row['CVR']}　
      <b>CPC：</b>{row['CPC']}　
      <b>CPM：</b>{row['CPM']}
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)
