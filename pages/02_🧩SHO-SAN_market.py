import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("📊 カテゴリ×都道府県 達成率モニター")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    kpi_df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta`").to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# 前処理
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
for col in ["Cost", "Clicks", "Impressions", "コンバージョン数"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

agg = df.groupby("CampaignId").agg({
    "Cost": "sum", "Clicks": "sum", "Impressions": "sum",
    "カテゴリ": "first", "広告目的": "first", "都道府県": "first", "CampaignName": "first"
}).reset_index()

agg["カテゴリ"] = agg["カテゴリ"].fillna("未設定")
agg["都道府県"] = agg["都道府県"].fillna("")

merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["最新CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["最新CV"]
merged["CPC"] = merged["Cost"] / merged["Clicks"]
merged["CPM"] = (merged["Cost"] / merged["Impressions"]) * 1000

# KPI型変換
kpi_numeric_cols = [col for col in kpi_df.columns if "_best" in col or "_good" in col or "_min" in col]
for col in kpi_numeric_cols:
    kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")

merged = pd.merge(merged, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# UI表示
selected_category = st.selectbox("📂 表示カテゴリ", sorted(merged["カテゴリ"].unique()))
cat_df = merged[merged["カテゴリ"] == selected_category]

tabs = st.tabs(["🎯 CPA", "🔁 CVR", "⚡ CTR", "🧮 CPC", "📡 CPM"])
metrics = {
    "🎯 CPA": ("CPA", "CPA_best"),
    "🔁 CVR": ("CVR", "CVR_best"),
    "⚡ CTR": ("CTR", "CTR_best"),
    "🧮 CPC": ("CPC", "CPC_best"),
    "📡 CPM": ("CPM", "CPM_best"),
}

for i, (label, (metric, goal_col)) in enumerate(metrics.items()):
    with tabs[i]:
        st.subheader(f"{label} 達成率グラフ")
        plot_df = cat_df[["CampaignName", metric, goal_col]].dropna()
        if plot_df.empty:
            st.info("⚠️ データがありません")
            continue
        plot_df["達成率"] = (plot_df[goal_col] / plot_df[metric]) * 100
        plot_df = plot_df.sort_values("達成率", ascending=True)
        plot_df["評価"] = plot_df["達成率"].apply(
            lambda x: "◎" if x >= 120 else "○" if x >= 100 else "△" if x >= 80 else "×"
        )
        color_map = {"◎": "green", "○": "yellowgreen", "△": "orange", "×": "red"}
        plot_df["色"] = plot_df["評価"].map(color_map)

        fig = px.bar(
            plot_df,
            y="CampaignName",
            x="達成率",
            color="評価",
            orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率"].map(lambda x: f"{x:.1f}%"),
            height=max(400, len(plot_df) * 40)
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            xaxis_title="達成率（%）",
            yaxis_title="",
            showlegend=True,
            margin=dict(t=40, b=40),
        )

        st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 サマリー"):
            summary = plot_df["評価"].value_counts().reindex(["◎", "○", "△", "×"], fill_value=0)
            avg = plot_df["達成率"].mean()
            st.markdown(f"""
            - 🎯 **目標（{goal_col}）**：{plot_df[goal_col].mean():,.2f}
            - 📦 **キャンペーン数**：{len(plot_df)}件
            - ✅ **平均達成率**：{avg:.1f}%
            - ◎：{summary["◎"]} 件
            - ○：{summary["○"]} 件
            - △：{summary["△"]} 件
            - ×：{summary["×"]} 件
            """)
