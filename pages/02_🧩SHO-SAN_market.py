
import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ設定
st.set_page_config(page_title="SHO-SANマーケット", layout="wide")
st.title("🌿 SHO-SAN 広告市場")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ読み込み
@st.cache_data
def load_data():
    query = """
    SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`
    """
    kpi_query = """
    SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators`
    """
    return client.query(query).to_dataframe(), client.query(kpi_query).to_dataframe()

df, kpi_df = load_data()

# 日付変換とフィルター
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
if not df["Date"].isnull().all():
    min_date = df["Date"].min().date()
    max_date = df["Date"].max().date()
    selected_range = st.sidebar.date_input("📅 日付で絞り込み", (min_date, max_date))
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        df = df[(df["Date"].dt.date >= selected_range[0]) & (df["Date"].dt.date <= selected_range[1])]

# フィルター
for col, label in {
    "都道府県": "🏙️ 都道府県",
    "カテゴリ": "🏷️ カテゴリ",
    "広告目的": "🎯 広告目的"
}.items():
    if col in df.columns:
        options = ["すべて"] + sorted(df[col].dropna().unique())
        selected = st.sidebar.selectbox(label, options, key=col)
        if selected != "すべて":
            df = df[df[col] == selected]

# 型変換
for col in ["Cost", "Clicks", "Impressions"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# キャンペーンごとの最新CV数（最新日付の行だけ使用）
latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

# キャンペーンごとの合計値（Cost, Clicks, Impressions）
agg = df.groupby("CampaignId").agg({
    "Cost": "sum",
    "Clicks": "sum",
    "Impressions": "sum",
    "カテゴリ": "first",
    "広告目的": "first"
}).reset_index()

# JOINしてKPI計算
merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["最新CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["最新CV"]
merged["CPC"] = merged["Cost"] / merged["Clicks"]
merged["CPM"] = (merged["Cost"] / merged["Impressions"]) * 1000

# KPI目標読み込み＆数値化
for col in ["CPA目標", "CVR目標", "CTR目標", "CPC目標", "CPM目標"]:
    if col in kpi_df.columns:
        kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")

# マージ（カテゴリ＋広告目的）
merged = pd.merge(merged, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# 評価ロジック
def evaluate(actual, target, higher_is_better=True):
    if pd.isna(actual) or pd.isna(target) or target == 0:
        return "-"
    if higher_is_better:
        if actual >= target * 1.2:
            return "◎"
        elif actual >= target:
            return "○"
        elif actual >= target * 0.8:
            return "△"
        else:
            return "×"
    else:
        if actual <= target * 0.8:
            return "◎"
        elif actual <= target:
            return "○"
        elif actual <= target * 1.2:
            return "△"
        else:
            return "×"

merged["CTR評価"] = merged.apply(lambda r: evaluate(r["CTR"], r["CTR目標"], True), axis=1)
merged["CVR評価"] = merged.apply(lambda r: evaluate(r["CVR"], r["CVR目標"], True), axis=1)
merged["CPA評価"] = merged.apply(lambda r: evaluate(r["CPA"], r["CPA目標"], False), axis=1)

# カテゴリ＋広告目的ごとに集計＆平均化（CV数は合計）
grouped = merged.groupby(["カテゴリ", "広告目的"]).agg({
    "CTR": "mean", "CTR目標": "mean", "CTR評価": "first",
    "CVR": "mean", "CVR目標": "mean", "CVR評価": "first",
    "CPA": "mean", "CPA目標": "mean", "CPA評価": "first"
}).reset_index()

st.subheader("📝 KPI評価＆ChatGPT風コメント")

for _, row in grouped.iterrows():
    st.markdown(f'''
    ### ■ {row["カテゴリ"]}（{row["広告目的"]}）  
    - CTR: {row["CTR"]:.2%}（目標 {row["CTR目標"]:.2%}） → {row["CTR評価"]}
    - CVR: {row["CVR"]:.2%}（目標 {row["CVR目標"]:.2%}） → {row["CVR評価"]}
    - CPA: ¥{row["CPA"]:,.0f}（目標 ¥{row["CPA目標"]:,.0f}） → {row["CPA評価"]}
    ''', unsafe_allow_html=True)

    msg = ""
    if row["CPA評価"] in ["◎", "○"]:
        msg += "✨ CPAが目標を達成していて、費用対効果の面でも好調ですね！\n"
    elif row["CPA評価"] == "△":
        msg += "😌 CPAは許容範囲内ですが、もう一歩で目標到達といったところです。\n"
    elif row["CPA評価"] == "×":
        msg += "⚠️ CPAが高めです。改善の余地がありそうですね。\n"

    if row["CTR評価"] in ["◎", "○"]:
        msg += "👍 CTRは良好で、広告の注目度は高いです。"
    elif row["CTR評価"] in ["△", "×"]:
        msg += "👀 CTRが低めなので、バナーや見出しの改善が効果的かもしれません。"

    st.info(msg)

st.subheader("📊 計算済み広告指標データ")
st.dataframe(merged, use_container_width=True)
