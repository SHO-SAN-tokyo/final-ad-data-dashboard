import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="SHO-SAN Market - 達成率分析", layout="wide")
st.title("📊 カテゴリ×都道府県 達成率モニター")

# BigQuery接続
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(info_dict)
client = bigquery.Client(credentials=credentials)

@st.cache_data
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    kpi_df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta`").to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# 前処理
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# 最新CV（キャンペーンごとに1件）
latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

# 集計
agg = df.groupby("CampaignId").agg({
    "Cost": "sum", "Clicks": "sum", "Impressions": "sum",
    "カテゴリ": "first", "広告目的": "first", "都道府県": "first"
}).reset_index()
agg["カテゴリ"] = agg["カテゴリ"].fillna("未設定")
agg["広告目的"] = agg["広告目的"].fillna("未設定")
agg["都道府県"] = agg["都道府県"].fillna("")

# 指標計算
merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["最新CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["最新CV"]
merged["CPC"] = merged["Cost"] / merged["Clicks"]
merged["CPM"] = (merged["Cost"] / merged["Impressions"]) * 1000

# KPI整形
for col in [
    "CPA_best", "CVR_best", "CTR_best", "CPC_best", "CPM_best",
    "CPA_good", "CVR_good", "CTR_good", "CPC_good", "CPM_good",
    "CPA_min", "CVR_min", "CTR_min", "CPC_min", "CPM_min"
]:
    if col in kpi_df.columns:
        kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")

merged = pd.merge(merged, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# 評価関数
def evaluate(metric_val, best, good, minimum, higher_is_better=True):
    if pd.isna(metric_val) or pd.isna(best) or pd.isna(good) or pd.isna(minimum):
        return None
    if higher_is_better:
        if metric_val >= best:
            return "◎"
        elif metric_val >= good:
            return "○"
        elif metric_val >= minimum:
            return "△"
        else:
            return "×"
    else:
        if metric_val <= best:
            return "◎"
        elif metric_val <= good:
            return "○"
        elif metric_val <= minimum:
            return "△"
        else:
            return "×"

# カテゴリ選択
selected_category = st.selectbox("📂 表示カテゴリ", sorted(merged["カテゴリ"].unique()))
cat_df = merged[merged["カテゴリ"] == selected_category]

# 指標別タブ
tabs = st.tabs(["🎯 CPA", "🔁 CVR", "⚡ CTR", "🧮 CPC", "📡 CPM"])
tab_map = {
    "🎯 CPA": ("CPA", "CPA_best", "CPA_good", "CPA_min", False),
    "🔁 CVR": ("CVR", "CVR_best", "CVR_good", "CVR_min", True),
    "⚡ CTR": ("CTR", "CTR_best", "CTR_good", "CTR_min", True),
    "🧮 CPC": ("CPC", "CPC_best", "CPC_good", "CPC_min", False),
    "📡 CPM": ("CPM", "CPM_best", "CPM_good", "CPM_min", False),
}

for label, (metric, best_col, good_col, min_col, higher_is_better) in tab_map.items():
    with tabs[list(tab_map.keys()).index(label)]:
        st.subheader(f"{label} 達成率グラフ")
        plot_df = cat_df[["都道府県", metric, best_col, good_col, min_col]].dropna()
        plot_df = plot_df[plot_df["都道府県"] != ""]

        if plot_df.empty:
            st.info("📭 表示できるデータがありません")
            continue

        plot_df["達成率"] = (plot_df[best_col] / plot_df[metric]) * 100
        plot_df["評価"] = plot_df.apply(lambda row: evaluate(row[metric], row[best_col], row[good_col], row[min_col], higher_is_better), axis=1)
        plot_df = plot_df[plot_df["評価"].notna()].sort_values("達成率", ascending=False)

        fig = px.bar(
            plot_df,
            x="達成率",
            y="都道府県",
            orientation="h",
            color="評価",
            color_discrete_map={"◎": "green", "○": "gold", "△": "gray", "×": "red"},
            text=plot_df["達成率"].apply(lambda x: f"{x:.1f}%"),
            title=f"{selected_category}｜{metric} 達成率"
        )

        fig.update_layout(
            xaxis_title="達成率（%）",
            yaxis_title="",
            title_font=dict(size=18),
            height=40 * len(plot_df) + 100,
            margin=dict(l=80, r=40, t=50, b=50),
            showlegend=True
        )

        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
