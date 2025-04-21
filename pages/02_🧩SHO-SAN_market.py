import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("📊 カテゴリ×都道府県 達成率モニター")

# BigQuery 認証
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
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

agg = df.groupby("CampaignId").agg({
    "Cost": "sum", "Clicks": "sum", "Impressions": "sum",
    "カテゴリ": "first", "広告目的": "first", "地方": "first",
    "都道府県": "first", "CampaignName": "first"
}).reset_index()

merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["最新CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["最新CV"]
merged["CPC"] = merged["Cost"] / merged["Clicks"]
merged["CPM"] = (merged["Cost"] / merged["Impressions"]) * 1000

for col in kpi_df.columns:
    if col not in ["カテゴリ", "広告目的"]:
        kpi_df[col] = pd.to_numeric(kpi_df[col], errors="coerce")
merged = pd.merge(merged, kpi_df, how="left", on=["カテゴリ", "広告目的"])

# フィルター
st.markdown("### 📂 条件を絞り込む")
col1, col2, col3, col4 = st.columns(4)

with col1:
    category = st.selectbox("カテゴリ", ["すべて"] + sorted(merged["カテゴリ"].dropna().unique()))
with col2:
    purpose = st.selectbox("広告目的", ["すべて"] + sorted(merged["広告目的"].dropna().unique()))
with col3:
    area = st.selectbox("地方", ["すべて"] + sorted(merged["地方"].dropna().unique()))

if area == "すべて":
    pref_list = ["すべて"] + sorted(merged["都道府県"].dropna().unique())
else:
    pref_list = ["すべて"] + sorted(merged[merged["地方"] == area]["都道府県"].dropna().unique())

with col4:
    pref = st.selectbox("都道府県", pref_list)

filtered_df = merged.copy()
if category != "すべて":
    filtered_df = filtered_df[filtered_df["カテゴリ"] == category]
if purpose != "すべて":
    filtered_df = filtered_df[filtered_df["広告目的"] == purpose]
if area != "すべて":
    filtered_df = filtered_df[filtered_df["地方"] == area]
if pref != "すべて":
    filtered_df = filtered_df[filtered_df["都道府県"] == pref]

# 指標タブ
tabs = st.tabs([
    "💰 CPA", "🔁 CVR", "⚡ CTR", "🧮 CPC", "📡 CPM"])
tab_map = {
    "💰 CPA": ("CPA", "CPA_best", "CPA_good", "CPA_min"),
    "🔁 CVR": ("CVR", "CVR_best", "CVR_good", "CVR_min"),
    "⚡ CTR": ("CTR", "CTR_best", "CTR_good", "CTR_min"),
    "🧮 CPC": ("CPC", "CPC_best", "CPC_good", "CPC_min"),
    "📡 CPM": ("CPM", "CPM_best", "CPM_good", "CPM_min")
}
color_map = {"◎": "#88c999", "○": "#d3dc74", "△": "#f3b77d", "×": "#e88c8c"}

for label, (metric, best_col, good_col, min_col) in tab_map.items():
    with tabs[list(tab_map.keys()).index(label)]:
        st.markdown(f"### {label} 達成率グラフ")
        plot_df = filtered_df[["都道府県", metric, best_col, good_col, min_col, "CampaignName"]].dropna()
        plot_df = plot_df[plot_df["都道府県"] != ""]
        if plot_df.empty:
            st.warning("📭 データがありません")
            continue

        plot_df["達成率"] = (plot_df[best_col] / plot_df[metric]) * 100

        def judge(row):
            val = row[metric]
            if pd.isna(val) or pd.isna(row[min_col]):
                return None
            if val <= row[best_col]: return "◎"
            elif val <= row[good_col]: return "○"
            elif val <= row[min_col]: return "△"
            else: return "×"

        plot_df["評価"] = plot_df.apply(judge, axis=1)

        total = len(plot_df)
        count_ok = (plot_df["評価"].isin(["◎", "○"])).sum()
        count_ng = (plot_df["評価"] == "×").sum()
        mean_val = plot_df[metric].mean()

        st.markdown(f"""
        <div style='display: flex; gap: 2rem; font-size: 15px; font-weight: bold; margin: 10px 0 20px 0;'>
            <div>🎯 目標値平均: ¥{plot_df[best_col].mean():,.0f}</div>
            <div>✅ 達成: {count_ok}件</div>
            <div>❌ 未達成: {count_ng}件</div>
            <div>📈 平均: ¥{mean_val:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

        plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")
        fig = px.bar(
            plot_df,
            y="ラベル",
            x="達成率",
            color="評価",
            orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else ""),
        )

        fig.update_traces(
            marker_line_width=0,
            textposition="outside",
            hovertemplate="%{y}<br>達成率: %{x:.1f}%<extra></extra>",
            width=0.2
        )

        fig.update_layout(
            xaxis_title="達成率（%）",
            yaxis_title="",
            height=180 + len(plot_df) * 13,
            width=950,
            margin=dict(t=40, l=60, r=40),
            showlegend=True,
            modebar=dict(remove=True),  # メニュー非表示
        )

        st.plotly_chart(fig, use_container_width=False)
