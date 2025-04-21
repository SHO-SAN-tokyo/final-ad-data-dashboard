import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("📊 カテゴリ×都道府県 達成率モニター")

# BigQuery接続
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    kpi_df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta`").to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# 日付型や数値型の変換
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# 🎯 日付フィルター（ページ上部）
st.subheader("📅 日付フィルター")
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
selected_date = st.date_input("期間を選択", (min_date, max_date), min_value=min_date, max_value=max_date)
if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
    start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
    df = df[(df["Date"].dt.date >= start_date.date()) & (df["Date"].dt.date <= end_date.date())]

# 📊 最新CV抽出
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

# 📂 絞り込み条件
st.markdown("<h5 style='margin-top: 2rem;'>📂 条件を絞り込む</h5>", unsafe_allow_html=True)
col1, col2, col3, col4 = st.columns(4)
category_options = ["すべて"] + sorted(merged["カテゴリ"].dropna().unique())
目的_options = ["すべて"] + sorted(merged["広告目的"].dropna().unique())
地方_options = ["すべて"] + sorted(merged["地方"].dropna().unique())

with col1:
    selected_category = st.selectbox("カテゴリ", category_options)
with col2:
    selected_objective = st.selectbox("広告目的", 目的_options)
with col3:
    selected_region = st.selectbox("地方", 地方_options)
with col4:
    都道府県候補 = merged[merged["地方"] == selected_region]["都道府県"].dropna().unique() if selected_region != "すべて" else merged["都道府県"].dropna().unique()
    selected_pref = st.selectbox("都道府県", ["すべて"] + sorted(都道府県候補))

# 条件反映
if selected_category != "すべて":
    merged = merged[merged["カテゴリ"] == selected_category]
if selected_objective != "すべて":
    merged = merged[merged["広告目的"] == selected_objective]
if selected_region != "すべて":
    merged = merged[merged["地方"] == selected_region]
if selected_pref != "すべて":
    merged = merged[merged["都道府県"] == selected_pref]

# 🎨 デザインとタブ設定
st.markdown("""
    <style>
    .tab-list p {
        padding: 0 20px;
    }
    section[data-testid="stHorizontalBlock"] > div {
        padding: 0 80px;
        justify-content: center !important;
    }
    section[data-testid="stHorizontalBlock"] div[role="tab"] {
        min-width: 180px !important;
        padding: 0.6rem 1.2rem;
        font-size: 1.1rem;
        justify-content: center;
    }
    .summary-card { display: flex; gap: 2rem; margin: 1rem 0 1.5rem 0; }
    .card {
        background: #f8f9fa;
        padding: 1rem 1.5rem;
        border-radius: 0.75rem;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        font-weight: bold; font-size: 1.1rem;
    }
    </style>
""", unsafe_allow_html=True)

tabs = st.tabs(["💰 CPA", "🔥 CVR", "⚡ CTR", "🧰 CPC", "📱 CPM"])
tab_map = {
    "💰 CPA": ("CPA", "CPA_best", "CPA_good", "CPA_min", "円"),
    "🔥 CVR": ("CVR", "CVR_best", "CVR_good", "CVR_min", "%"),
    "⚡ CTR": ("CTR", "CTR_best", "CTR_good", "CTR_min", "%"),
    "🧰 CPC": ("CPC", "CPC_best", "CPC_good", "CPC_min", "円"),
    "📱 CPM": ("CPM", "CPM_best", "CPM_good", "CPM_min", "円")
}
color_map = {"◎": "#88c999", "○": "#d3dc74", "△": "#f3b77d", "×": "#e88c8c"}

for label, (metric, best_col, good_col, min_col, unit) in tab_map.items():
    with tabs[list(tab_map.keys()).index(label)]:
        st.markdown(f"### {label} 達成率グラフ")
        plot_df = merged[["都道府県", metric, best_col, good_col, min_col, "CampaignName"]].dropna()
        plot_df = plot_df[plot_df["都道府県"] != ""]

        if plot_df.empty:
            st.warning("📭 データがありません")
            continue

        plot_df["達成率"] = (plot_df[best_col] / plot_df[metric]) * 100
        def judge(row):
            val = row[metric]
            if pd.isna(val) or pd.isna(row[min_col]): return None
            if val <= row[best_col]: return "◎"
            elif val <= row[good_col]: return "○"
            elif val <= row[min_col]: return "△"
            else: return "×"
        plot_df["評価"] = plot_df.apply(judge, axis=1)

        # サマリーカード
        count_ok = (plot_df["評価"].isin(["◎", "○"])).sum()
        count_ng = (plot_df["評価"] == "×").sum()
        mean_val = plot_df[metric].mean()
        avg_goal = plot_df[best_col].mean()

        st.markdown(f"""
        <div class="summary-card">
            <div class="card">🎯 目標値: {avg_goal:,.0f}{unit}</div>
            <div class="card">✅ 達成: {count_ok}件</div>
            <div class="card">❌ 未達成: {count_ng}件</div>
            <div class="card">📈 平均: {mean_val:,.0f}{unit}</div>
        </div>
        """, unsafe_allow_html=True)

        # グラフ描画
        plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")
        fig = px.bar(
            plot_df,
            y="ラベル",
            x="達成率",
            color="評価",
            orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率"].map(lambda x: f"{x:.1f}%" if pd.notna(x) else ""),
            custom_data=[plot_df[metric]]
        )
        fig.update_traces(
            textposition="outside", marker_line_width=0, width=0.25,
            hovertemplate="<b>%{y}</b><br>実績値: %{customdata[0]:,.0f}" + unit + "<br>達成率: %{x:.1f}%<extra></extra>"
        )
        fig.update_layout(
            xaxis_title="達成率（%）", yaxis_title="", showlegend=True,
            height=200 + len(plot_df) * 40,
            width=1000, margin=dict(t=40, l=60, r=20), modebar=dict(remove=True)
        )
        st.plotly_chart(fig, use_container_width=False)
