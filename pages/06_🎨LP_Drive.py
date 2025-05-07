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
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# --- フィルター ---
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
col1, col2, col3 = st.columns(3)

with col1:
    sel_date = st.date_input("📅 日付", (dmin, dmax), min_value=dmin, max_value=dmax)
with col2:
    cat_opts = ["すべて"] + sorted(df["カテゴリ"].dropna().unique())
    sel_cat = st.selectbox("🗂️ カテゴリ", cat_opts)
with col3:
    obj_opts = ["すべて"] + sorted(df["広告目的"].dropna().unique())
    sel_obj = st.selectbox("🎯 広告目的", obj_opts)

if isinstance(sel_date, tuple):
    s, e = pd.to_datetime(sel_date[0]), pd.to_datetime(sel_date[1])
    df = df[(df["Date"] >= s) & (df["Date"] <= e)]
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
    "CPA_best": "best", "CPA_good": "good", "CPA_min": "min"
})

# --- 評価関数 ---
def eval_cpa(row):
    match = target_df[
        (target_df["カテゴリ"] == row["カテゴリ"]) &
        (target_df["広告目的"] == row["広告目的"])
    ]
    if match.empty or pd.isna(row["CPA"]): return "-"
    b, g, m = map(lambda x: float(x) if pd.notna(x) else None, match.iloc[0][["best", "good", "min"]])
    v = row["CPA"]
    if b and v <= b: return "◎"
    if g and v <= g: return "◯"
    if m and v <= m: return "△"
    return "×"

# --- 評価列追加 ---
agg = agg.merge(df_latest[["CreativeDestinationUrl", "カテゴリ", "広告目的"]].drop_duplicates(), on="CreativeDestinationUrl", how="left")
agg["評価"] = agg.apply(eval_cpa, axis=1)

# --- 表示 ---
st.markdown("<h4 style='margin-top:2rem;'>📊 LPごとの集計</h4>", unsafe_allow_html=True)
st.dataframe(
    agg[["CreativeDestinationUrl", "カテゴリ", "広告目的", "Cost", "コンバージョン数", "CPA", "CTR", "CVR", "CPC", "CPM", "評価"]]
    .sort_values("Cost", ascending=False)
    .rename(columns={
        "CreativeDestinationUrl": "LP URL",
        "Cost": "消化金額", "コンバージョン数": "CV数"
    }),
    use_container_width=True,
    hide_index=True
)
