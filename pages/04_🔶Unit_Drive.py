import streamlit as st
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="🔶 Unit Drive", layout="wide")
st.title("🔶 Unit別パフォーマンスダッシュボード")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data(ttl=300)
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    unit_df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.UnitMapping`").to_dataframe()
    return df, unit_df

df, unit_df = load_data()

# 📅 日付フィルター
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
selected_range = st.date_input("📅 日付フィルター", (min_date, max_date), min_value=min_date, max_value=max_date)

if isinstance(selected_range, tuple) and len(selected_range) == 2:
    start_date, end_date = pd.to_datetime(selected_range[0]), pd.to_datetime(selected_range[1])
    df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# 最新行（1キャンペーンにつき1行）
latest = df.sort_values("Date").dropna(subset=["Date"])
latest = latest.loc[latest.groupby("CampaignId")["Date"].idxmax()]
latest = latest[["CampaignId", "担当者", "フロント", "予算", "フィー", "コンバージョン数"]]

# 消化金額だけは合計
agg = df.groupby("CampaignId").agg({"Cost": "sum"}).reset_index()

# 担当者→ユニット名結合
unit_df = unit_df.rename(columns={"所属": "Unit"})
latest = latest.merge(unit_df[["担当者", "Unit"]], on="担当者", how="left")

# 統合
merged = agg.merge(latest, on="CampaignId", how="left")
merged["予算"] = pd.to_numeric(merged["予算"], errors="coerce").fillna(0)
merged["フィー"] = pd.to_numeric(merged["フィー"], errors="coerce").fillna(0)
merged["コンバージョン数"] = pd.to_numeric(merged["コンバージョン数"], errors="coerce").fillna(0)
merged["CPA"] = merged["Cost"] / merged["コンバージョン数"]
merged = merged.replace([float("inf"), -float("inf")], pd.NA)

# 🎯 フィルター
st.markdown("### 🔍 条件で絞り込み")
col1, col2, col3 = st.columns(3)
units = ["すべて"] + sorted(merged["Unit"].dropna().unique())
members = ["すべて"] + sorted(merged["担当者"].dropna().unique())
fronts = ["すべて"] + sorted(merged["フロント"].dropna().unique())

with col1:
    selected_unit = st.selectbox("Unit", units)
with col2:
    selected_person = st.selectbox("担当者", members)
with col3:
    selected_front = st.selectbox("フロント", fronts)

if selected_unit != "すべて":
    merged = merged[merged["Unit"] == selected_unit]
if selected_person != "すべて":
    merged = merged[merged["担当者"] == selected_person]
if selected_front != "すべて":
    merged = merged[merged["フロント"] == selected_front]

# 📦 集計
summary = merged.groupby("Unit").agg({
    "CPA": "mean",
    "CampaignId": "nunique",
    "予算": "sum",
    "Cost": "sum",
    "フィー": "sum",
    "コンバージョン数": "sum"
}).reset_index()

summary = summary.fillna(0).sort_values("Unit")

# 💠 カードレイアウト
st.markdown("### 📦 ユニット別スコアカード")
colors = ["#A8C0D6", "#B4E0C1", "#D0E2B6", "#E0C0A2", "#CAB8D9", "#E3B6B6"]
col_count = 3
rows = [summary.iloc[i:i + col_count] for i in range(0, len(summary), col_count)]

for row in rows:
    cols = st.columns(col_count)
    for i, (_, r) in enumerate(row.iterrows()):
        with cols[i]:
            st.markdown(f"""
                <div style="background: {colors[i % len(colors)]}; padding: 1.2rem; border-radius: 1rem;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center;">
                    <div style="font-size: 1.4rem; font-weight: bold;">{r['Unit']}</div>
                    <div style="font-size: 1.3rem; font-weight: bold; margin: 0.4rem 0;">CPA: ¥{r['CPA']:,.0f}</div>
                    <div style="font-size: 0.9rem; line-height: 1.5rem;">
                        キャンペーン数: {int(r['CampaignId'])}<br>
                        予算: ¥{r['予算']:,.0f}<br>
                        消化金額: ¥{r['Cost']:,.0f}<br>
                        フィー: ¥{r['フィー']:,.0f}<br>
                        コンバージョン数: {int(r['コンバージョン数'])}
                    </div>
                </div>
            """, unsafe_allow_html=True)
