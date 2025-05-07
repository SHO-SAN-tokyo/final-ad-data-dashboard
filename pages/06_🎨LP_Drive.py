import streamlit as st
from google.cloud import bigquery
import pandas as pd, numpy as np

# --- ページ設定 ---
st.set_page_config(page_title="LP_Drive", layout="wide")
st.title("📄 LP別集計ダッシュボード")

# --- 認証 & 接続 ---
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

# --- データ取得 ---
query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
df = bq.query(query).to_dataframe()
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# --- 前処理 ---
df["カテゴリ"] = df["カテゴリ"].astype(str).fillna("未設定")
df["広告目的"] = df["広告目的"].astype(str).fillna("未設定")
df["CreativeDestinationUrl"] = df["CreativeDestinationUrl"].astype(str).fillna("未設定")
df["キャンペーンID"] = df["CampaignId"].astype(str).fillna("")
df["client_name"] = df["client_name"].astype(str).fillna("未設定")
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# --- フィルター ---
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    sel_date = st.date_input("📅 日付", (dmin, dmax), min_value=dmin, max_value=dmax)
with col2:
    client_opts = ["すべて"] + sorted(df["client_name"].dropna().unique())
    sel_client = st.selectbox("👤 クライアント名", client_opts)
with col3:
    cat_opts = ["すべて"] + sorted(df["カテゴリ"].dropna().unique())
    sel_cat = st.selectbox("🗂️ カテゴリ", cat_opts)
with col4:
    obj_opts = ["すべて"] + sorted(df["広告目的"].dropna().unique())
    sel_obj = st.selectbox("🎯 広告目的", obj_opts)
with col5:
    pass

if isinstance(sel_date, tuple):
    s, e = pd.to_datetime(sel_date[0]), pd.to_datetime(sel_date[1])
    df = df[(df["Date"] >= s) & (df["Date"] <= e)]
if sel_client != "すべて":
    df = df[df["client_name"] == sel_client]
if sel_cat != "すべて":
    df = df[df["カテゴリ"] == sel_cat]
if sel_obj != "すべて":
    df = df[df["広告目的"] == sel_obj]

# --- 最新のCVのみ取得 ---
latest_idx = df.sort_values("Date").groupby("キャンペーンID")["Date"].idxmax()
df_latest = df.loc[latest_idx].copy()

# --- 指標算出 ---
agg = df.groupby("CreativeDestinationUrl").agg({
    "Cost": "sum",
    "Impressions": "sum",
    "Clicks": "sum"
}).reset_index()

cv_df = df_latest.groupby("CreativeDestinationUrl")["コンバージョン数"].sum().reset_index()
agg = agg.merge(cv_df, on="CreativeDestinationUrl", how="left")

agg["CPA"] = agg["Cost"] / agg["コンバージョン数"].replace(0, np.nan)
agg["CTR"] = agg["Clicks"] / agg["Impressions"].replace(0, np.nan)
agg["CVR"] = agg["コンバージョン数"] / agg["Clicks"].replace(0, np.nan)
agg["CPC"] = agg["Cost"] / agg["Clicks"].replace(0, np.nan)
agg["CPM"] = (agg["Cost"] / agg["Impressions"].replace(0, np.nan)) * 1000

# --- 評価指標取得 ---
target_query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta"
target_df = bq.query(target_query).to_dataframe()
target_df = target_df.rename(columns={
    "CPA_best": "cpa_best", "CPA_good": "cpa_good", "CPA_min": "cpa_min",
    "CVR_best": "cvr_best", "CVR_good": "cvr_good", "CVR_min": "cvr_min"
})

# --- 評価関数 ---
def eval_metric(value, best, good, minv, reverse=False):
    if pd.isna(value) or pd.isna(best): return "-"
    if reverse:
        if value >= best: return "◎"
        if value >= good: return "◯"
        if value >= minv: return "△"
    else:
        if value <= best: return "◎"
        if value <= good: return "◯"
        if value <= minv: return "△"
    return "×"

def eval_row(row):
    match = target_df[
        (target_df["カテゴリ"] == row["カテゴリ"]) &
        (target_df["広告目的"] == row["広告目的"])
    ]
    if match.empty: return pd.Series(["-", "-"])
    m = match.iloc[0]
    cpa_eval = eval_metric(row["CPA"], m["cpa_best"], m["cpa_good"], m["cpa_min"], reverse=False)
    cvr_eval = eval_metric(row["CVR"], m["cvr_best"], m["cvr_good"], m["cvr_min"], reverse=True)
    return pd.Series([cpa_eval, cvr_eval])

# --- 評価列追加 ---
agg = agg.merge(df_latest[["CreativeDestinationUrl", "カテゴリ", "広告目的"]].drop_duplicates(), on="CreativeDestinationUrl", how="left")
if not agg.empty:
    agg[["CPA評価", "CVR評価"]] = agg.apply(eval_row, axis=1)
else:
    agg["CPA評価"] = []
    agg["CVR評価"] = []

# --- 書式整形 ---
agg["消化金額"] = agg["Cost"].apply(lambda x: f"{x:,.0f}円")
agg["CV数"] = agg["コンバージョン数"].astype(int)
agg["CPA"] = agg["CPA"].apply(lambda x: f"{x:,.0f}円" if pd.notna(x) else "-")
agg["CTR"] = agg["CTR"].apply(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "-")
agg["CVR"] = agg["CVR"].apply(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "-")
agg["CPC"] = agg["CPC"].apply(lambda x: f"{x:,.0f}円" if pd.notna(x) else "-")
agg["CPM"] = agg["CPM"].apply(lambda x: f"{x:,.0f}円" if pd.notna(x) else "-")

# --- 表示 ---
st.markdown("<h4 style='margin-top:2rem;'>📊 LPごとの集計</h4>", unsafe_allow_html=True)

for _, row in agg.sort_values("Cost", ascending=False).iterrows():
    st.markdown(f"""
    <div style='margin-bottom:1.5rem;'>
      <a href="{row['CreativeDestinationUrl']}" target="_blank">🔗 {row['CreativeDestinationUrl']}</a><br>
      <b>カテゴリ：</b>{row['カテゴリ']}　<b>広告目的：</b>{row['広告目的']}　<b>CPA評価：</b>{row['CPA評価']}　<b>CVR評価：</b>{row['CVR評価']}<br>
      <b>消化金額：</b>{row['消化金額']}　<b>CV数：</b>{row['CV数']}　<b>CPA：</b>{row['CPA']}　<b>CTR：</b>{row['CTR']}　<b>CVR：</b>{row['CVR']}　<b>CPC：</b>{row['CPC']}　<b>CPM：</b>{row['CPM']}
    </div>
    """, unsafe_allow_html=True)
