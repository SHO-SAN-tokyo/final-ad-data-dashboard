import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np
import html

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

# --- ページ設定 ---
st.set_page_config(page_title="LP_Score", layout="wide")
st.title("🎨 LP Score")
st.markdown("###### LP（ランディングページ/URL）単位での広告スコアを集計します。")

# --- 認証 & 接続 ---
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

# --- データ取得 ---
@st.cache_data(ttl=60)
def load_lp_data():
    query = """
        SELECT
          client_name,
          `URL`,
          `メインカテゴリ`,
          `サブカテゴリ`,
          `広告目的`,
          `広告媒体`,
          Cost,
          Impressions,
          Clicks,
          `コンバージョン数`,
          CPA,
          CPC,
          CPM,
          CVR,
          CTR
        FROM careful-chess-406412.SHOSAN_Ad_Tokyo.LP_Score_Ready
        ORDER BY Cost DESC
    """
    return bq.query(query).to_dataframe()

df = load_lp_data()
if df.empty:
    st.warning("⚠️ データがありません")
    st.stop()

# --- 前処理 ---
def format_num(x, is_money=False):
    if pd.isna(x): return "-"
    if is_money: return f"{x:,.0f}円"
    return f"{x:,.0f}"

def format_percent(x):
    if pd.isna(x): return "-"
    return f"{x*100:.2f}%"

def make_link(url):
    if pd.isna(url) or not str(url).startswith("http"):
        return "-"
    esc_url = html.escape(url)
    return f'<a href="{esc_url}" target="_blank">{esc_url}</a>'

# --- フィルター ---
client_opts = ["すべて"] + sorted(df["client_name"].dropna().unique())
main_cat_opts = ["すべて"] + sorted(df["メインカテゴリ"].dropna().unique())
sub_cat_opts = ["すべて"] + sorted(df["サブカテゴリ"].dropna().unique())
purpose_opts = ["すべて"] + sorted(df["広告目的"].dropna().unique())
media_opts = ["すべて"] + sorted(df["広告媒体"].dropna().unique())

col1, col2, col3, col4, col5 = st.columns(5)
sel_client = col1.selectbox("クライアント名", client_opts, index=0)
sel_main = col2.selectbox("メインカテゴリ", main_cat_opts, index=0)
sel_sub = col3.selectbox("サブカテゴリ", sub_cat_opts, index=0)
sel_purpose = col4.selectbox("広告目的", purpose_opts, index=0)
sel_media = col5.selectbox("広告媒体", media_opts, index=0)

# --- フィルタリング ---
filtered = df.copy()
if sel_client != "すべて":
    filtered = filtered[filtered["client_name"] == sel_client]
if sel_main != "すべて":
    filtered = filtered[filtered["メインカテゴリ"] == sel_main]
if sel_sub != "すべて":
    filtered = filtered[filtered["サブカテゴリ"] == sel_sub]
if sel_purpose != "すべて":
    filtered = filtered[filtered["広告目的"] == sel_purpose]
if sel_media != "すべて":
    filtered = filtered[filtered["広告媒体"] == sel_media]

# --- フィルター結果サマリー ---
def join_or_all(val):
    if isinstance(val, list):
        return "、".join(val) if val else "すべて"
    return val if val and val != "すべて" else "すべて"

st.markdown(
    f"""
    <div style="font-size:13px; margin: 0 0 18px 0; color:#15519d; padding:8px 12px 7px 12px; border-radius:8px; background: #f4f7fa; border:1px solid #dbeafe;">
        👤 クライアント名：<b>{join_or_all([sel_client])}</b>　
        📁 メインカテゴリ：<b>{join_or_all([sel_main])}</b>　
        📂 サブカテゴリ：<b>{join_or_all([sel_sub])}</b>　
        🎯 広告目的：<b>{join_or_all([sel_purpose])}</b>　
        📡 広告媒体：<b>{join_or_all([sel_media])}</b>
    </div>
    """,
    unsafe_allow_html=True,
)

# --- 書式整形 ---
show_df = filtered.copy()
show_df["URL"] = show_df["URL"].apply(make_link)
show_df["Cost"] = show_df["Cost"].apply(lambda x: format_num(x, is_money=True))
show_df["CPA"] = show_df["CPA"].apply(lambda x: format_num(x, is_money=True))
show_df["CPC"] = show_df["CPC"].apply(lambda x: format_num(x, is_money=True))
show_df["CPM"] = show_df["CPM"].apply(lambda x: format_num(x, is_money=True))
show_df["CTR"] = show_df["CTR"].apply(format_percent)
show_df["CVR"] = show_df["CVR"].apply(format_percent)

disp_cols = [
    "URL", "client_name", "メインカテゴリ", "サブカテゴリ", "広告目的", "広告媒体",
    "Cost", "Impressions", "Clicks", "コンバージョン数", "CPA", "CPC", "CVR", "CTR", "CPM"
]
show_df = show_df[disp_cols]

# --- 表示 ---
st.markdown("<h4 style='margin-top:2rem;'>📊 LP（URL）ごとの集計</h4>", unsafe_allow_html=True)
for _, row in show_df.iterrows():
    card_html = f"""
    <div style='border:1px solid #ddd; border-radius:10px; padding:16px; margin-bottom:16px; background:#fdfdfd; font-size: 14px;'>
      <div><b>URL：</b>{row['URL']}</div>
      <div>
        <b>クライアント名：</b>{row['client_name']}　
        <b>メインカテゴリ：</b>{row['メインカテゴリ']}　
        <b>サブカテゴリ：</b>{row['サブカテゴリ']}　
        <b>広告目的：</b>{row['広告目的']}　
        <b>広告媒体：</b>{row['広告媒体']}
      </div>
      <div>
        <b>消化金額：</b>{row['Cost']}　
        <b>CV数：</b>{row['コンバージョン数']}　
        <b>CPA：</b>{row['CPA']}　
        <b>CTR：</b>{row['CTR']}　
        <b>CVR：</b>{row['CVR']}　
        <b>CPC：</b>{row['CPC']}　
        <b>CPM：</b>{row['CPM']}
      </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)
