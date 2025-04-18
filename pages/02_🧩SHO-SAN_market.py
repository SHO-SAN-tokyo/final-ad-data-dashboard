
import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ページ設定
st.set_page_config(page_title="SHO-SANマーケット", layout="wide")
st.title("🌿 SHO-SAN 広告市場")

# BigQuery 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ読み込み
@st.cache_data
def load_data():
    ad_query = """
    SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`
    """
    kpi_query = """
    SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators`
    """
    return client.query(ad_query).to_dataframe(), client.query(kpi_query).to_dataframe()

df, kpi_df = load_data()

# 日付フィルター
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if not df["Date"].isnull().all():
        min_date = df["Date"].min().date()
        max_date = df["Date"].max().date()
        selected_range = st.sidebar.date_input("📅 日付で絞り込み", (min_date, max_date))
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            df = df[(df["Date"].dt.date >= selected_range[0]) & (df["Date"].dt.date <= selected_range[1])]

# 絞り込みフィルター
filters = {
    "都道府県": "🏙️ 都道府県を選択",
    "カテゴリ": "🏷️ カテゴリを選択",
    "広告目的": "🎯 広告目的を選択"
}
for col, label in filters.items():
    if col in df.columns:
        options = ["すべて"] + sorted(df[col].dropna().unique())
        choice = st.sidebar.selectbox(label, options, key=col)
        if choice != "すべて":
            df = df[df[col] == choice]

# 数値列変換
for col in ["CTR", "CVR", "CPA", "CPC", "CPM"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

for col in ["CTR目標", "CVR目標", "CPA目標", "CPC目標", "CPM目標"]:
    kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")

# KPI結合
missing_cols = [col for col in ["カテゴリ", "広告目的"] if col not in df.columns or col not in kpi_df.columns]
if missing_cols:
    st.error(f"必要な列が見つかりません: {', '.join(missing_cols)}")
    st.stop()

merged = pd.merge(df, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# 評価関数
def evaluate(actual, target, higher_is_better=True):
    if pd.isna(actual) or pd.isna(target):
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

# グルーピング
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

st.subheader("📊 KPI比較付きデータ")
st.dataframe(merged, use_container_width=True)
