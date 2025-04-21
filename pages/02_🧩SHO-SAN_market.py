
import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="SHO-SANマーケット", layout="wide")
st.title("🌿 SHO-SAN 広告市場（地方別 KPI）")

# BigQuery認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    kpi = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators`").to_dataframe()
    return df, kpi

df, kpi_df = load_data()

# 前処理
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)

# 最新CV抽出
latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

# 合計値
agg = df.groupby("CampaignId").agg({
    "Cost": "sum", "Clicks": "sum", "Impressions": "sum",
    "カテゴリ": "first", "広告目的": "first",
    "都道府県": "first", "地方": "first"
}).reset_index()

# 欠損値補完
agg["カテゴリ"] = agg["カテゴリ"].fillna("未設定")
agg["広告目的"] = agg["広告目的"].fillna("未設定")
agg["地方"] = agg["地方"].fillna("未設定")
agg["都道府県"] = agg["都道府県"].fillna("")

# 指標計算
merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["最新CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["最新CV"]
merged["CPC"] = merged["Cost"] / merged["Clicks"]
merged["CPM"] = (merged["Cost"] / merged["Impressions"]) * 1000

# KPI目標の型変換と結合
for col in ["CPA目標", "CVR目標", "CTR目標", "CPC目標", "CPM目標"]:
    kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")
merged = pd.merge(merged, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# 評価関数
def evaluate(actual, target, higher_is_better=True):
    if pd.isna(actual) or pd.isna(target) or target == 0:
        return "-"
    if higher_is_better:
        if actual >= target * 1.2: return "◎"
        elif actual >= target: return "○"
        elif actual >= target * 0.8: return "△"
        else: return "×"
    else:
        if actual <= target * 0.8: return "◎"
        elif actual <= target: return "○"
        elif actual <= target * 1.2: return "△"
        else: return "×"

merged["CTR評価"] = merged.apply(lambda r: evaluate(r["CTR"], r["CTR目標"], True), axis=1)
merged["CVR評価"] = merged.apply(lambda r: evaluate(r["CVR"], r["CVR目標"], True), axis=1)
merged["CPA評価"] = merged.apply(lambda r: evaluate(r["CPA"], r["CPA目標"], False), axis=1)

# 表示対象データ
display_df = merged.copy()

st.subheader("📊 地方別 KPI評価（未設定も表示）")

for region in sorted(display_df["地方"].unique()):
    st.markdown(f"## 🏯 {region}")
    region_df = display_df[display_df["地方"] == region]
    cols = st.columns(2)

    for i, (_, row) in enumerate(region_df.iterrows()):
        ctr_goal = f"{row['CTR目標']:.2%}" if pd.notna(row["CTR目標"]) else "未設定"
        cvr_goal = f"{row['CVR目標']:.2%}" if pd.notna(row["CVR目標"]) else "未設定"
        cpa_goal = f"¥{row['CPA目標']:,.0f}" if pd.notna(row["CPA目標"]) else "未設定"
        pref_display = f"<b>{row['都道府県']}</b>｜" if row["都道府県"] else ""

        with cols[i % 2]:
            st.markdown(f'''
<div style="background-color:#f7f9fc; padding:15px; border-radius:10px; margin:10px 0; box-shadow:0 2px 4px rgba(0,0,0,0.06);">
  <h4 style="margin-bottom:10px;">📍 {pref_display}{row["カテゴリ"]}（{row["広告目的"]}）</h4>
  <ul style="list-style:none; padding-left:0; font-size:15px;">
    <li>CTR：{row["CTR"]:.2%}（目標 {ctr_goal}） → {row["CTR評価"]}</li>
    <li>CVR：{row["CVR"]:.2%}（目標 {cvr_goal}） → {row["CVR評価"]}</li>
    <li>CPA：¥{row["CPA"]:,.0f}（目標 {cpa_goal}） → {row["CPA評価"]}</li>
  </ul>
</div>
''', unsafe_allow_html=True)
