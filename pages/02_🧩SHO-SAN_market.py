import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🪩SHO-SAN market")
st.subheader("📊 カテゴリ × 都道府県  キャンペーン達成率モニター")

# BigQuery接続
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data
def load_data():
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data").to_dataframe()
    kpi_df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta").to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# 前処理
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# 🗓 日付フィルター
st.markdown("<h5 style='margin-top: 2rem;'>🗓 日付フィルター</h5>", unsafe_allow_html=True)
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
selected_date = st.date_input("期間を選択", (min_date, max_date), min_value=min_date, max_value=max_date)
if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
    start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
    df = df[(df["Date"].dt.date >= start_date.date()) & (df["Date"].dt.date <= end_date.date())]

# 最新CV
latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

# 集計と指標計算
agg = df.groupby("CampaignId").agg({
    "Cost": "sum", "Clicks": "sum", "Impressions": "sum",
    "カテゴリ": "first", "広告目的": "first", "都道府県": "first", "地方": "first", "CampaignName": "first"
}).reset_index()
merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["最新CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["最新CV"]
merged["CPC"] = merged["Cost"] / merged["Clicks"]
merged["CPM"] = (merged["Cost"] / merged["Impressions"]) * 1000

# KPIマージ
goal_cols = [
    "CPA_best", "CPA_good", "CPA_min",
    "CVR_best", "CVR_good", "CVR_min",
    "CTR_best", "CTR_good", "CTR_min",
    "CPC_best", "CPC_good", "CPC_min",
    "CPM_best", "CPM_good", "CPM_min"
]
for col in goal_cols:
    if col in kpi_df.columns:
        kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")
merged = pd.merge(merged, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# CVR用のフォーマット
label = "🔥 CVR"
metric, best_col, good_col, min_col, unit = "CVR", "CVR_best", "CVR_good", "CVR_min", "%"

st.markdown(f"### {label} 達成率グラフ")
merged["達成率"] = (merged[best_col] / merged[metric]) * 100

# 判定ロジック
merged["評価"] = merged.apply(lambda row:
    "◎" if row[metric] >= row[best_col] else
    "○" if row[metric] >= row[good_col] else
    "△" if row[metric] >= row[min_col] else
    "×", axis=1)

plot_df = merged[["都道府県", metric, best_col, good_col, min_col, "CampaignName", "達成率", "評価"]].dropna()
plot_df = plot_df[plot_df["都道府県"] != ""]

count_high = (plot_df["評価"] == "◎").sum()
count_good = (plot_df["評価"] == "○").sum()
count_mid = (plot_df["評価"] == "△").sum()
count_ng = (plot_df["評価"] == "×").sum()
mean_val = plot_df[metric].mean()
avg_goal = plot_df[best_col].mean()

st.markdown(f"""
<div class="summary-card">
    <div class="card">🎯 目標値<br><div class="value">{avg_goal:.2%}</div></div>
    <div class="card">💎 ハイ達成<br><div class="value">{count_high}件</div></div>
    <div class="card">🟢 通常達成<br><div class="value">{count_good}件</div></div>
    <div class="card">🟡 もう少し<br><div class="value">{count_mid}件</div></div>
    <div class="card">✖️ 未達成<br><div class="value">{count_ng}件</div></div>
    <div class="card">📈 実績値<br><div class="value">{mean_val:.2%}</div></div>
</div>
""", unsafe_allow_html=True)

plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")
fig = px.bar(
    plot_df,
    y="ラベル",
    x="達成率",
    color="評価",
    orientation="h",
    color_discrete_map={"◎": "#88c999", "○": "#d3dc74", "△": "#f3b77d", "×": "#e88c8c"},
    text=plot_df["達成率"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else ""),
    custom_data=[plot_df[metric]]
)
fig.update_traces(
    textposition="outside", marker_line_width=0, width=0.25,
    hovertemplate="<b>%{y}</b><br>実績値: %{customdata[0]:.2%}<br>達成率: %{x:.1f}%<extra></extra>",
    textfont_size=14
)
fig.update_layout(
    xaxis_title="達成率 (‰)", yaxis_title="", showlegend=True,
    height=200 + len(plot_df) * 40, width=1000,
    margin=dict(t=40, l=60, r=20), modebar=dict(remove=True),
    font=dict(size=14)
)
st.plotly_chart(fig, use_container_width=False)
