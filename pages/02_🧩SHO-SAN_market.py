import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧹SHO-SAN market")
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
df["\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"] = pd.to_numeric(df["\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"], errors="coerce").fillna(0)

# 日付フィルター
min_date = df["Date"].min().date()
max_date = df["Date"].max().date()
selected_date = st.date_input("期間を選択", (min_date, max_date), min_value=min_date, max_value=max_date)
if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
    start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
    df = df[(df["Date"].dt.date >= start_date.date()) & (df["Date"].dt.date <= end_date.date())]

# 最新CV
latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"]].rename(columns={"\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570": "\u6700\u65b0CV"})

# 指標計算
agg = df.groupby("CampaignId").agg({
    "Cost": "sum", "Clicks": "sum", "Impressions": "sum",
    "\u30ab\u30c6\u30b4\u30ea": "first", "\u5e83\u544a\u76ee\u7684": "first", "\u90fd\u9053\u5e9c\u770c": "first", "\u5730\u65b9": "first", "CampaignName": "first"
}).reset_index()
merged = pd.merge(agg, latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"]
merged["CVR"] = merged["\u6700\u65b0CV"] / merged["Clicks"]
merged["CPA"] = merged["Cost"] / merged["\u6700\u65b0CV"]
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
merged = pd.merge(merged, kpi_df, how="left", on=["\u30ab\u30c6\u30b4\u30ea", "\u5e83\u544a\u76ee\u7684"])

# --- ここでCVRタブのみ個別処理 ---
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

        if label == "🔥 CVR":
            merged["達成率"] = (merged[metric] / merged[best_col]) * 100
            def judge(row):
                val = row[metric]
                if pd.isna(val) or pd.isna(row[min_col]): return None
                if val >= row[best_col]: return "◎"
                elif val >= row[good_col]: return "○"
                elif val >= row[min_col]: return "△"
                else: return "×"
        else:
            merged["達成率"] = (merged[best_col] / merged[metric]) * 100
            def judge(row):
                val = row[metric]
                if pd.isna(val) or pd.isna(row[min_col]): return None
                if val <= row[best_col]: return "◎"
                elif val <= row[good_col]: return "○"
                elif val <= row[min_col]: return "△"
                else: return "×"

        merged["評価"] = merged.apply(judge, axis=1)

        plot_df = merged[["都道府県", metric, best_col, good_col, min_col, "CampaignName", "達成率", "評価"]].dropna()
        plot_df = plot_df[plot_df["都道府県"] != ""]

        if plot_df.empty:
            st.warning("📭 データがありません")
            continue

        count_high = (plot_df["評価"] == "◎").sum()
        count_good = (plot_df["評価"] == "○").sum()
        count_mid = (plot_df["評価"] == "△").sum()
        count_ng = (plot_df["評価"] == "×").sum()
        mean_val = plot_df[metric].mean()
        avg_goal = plot_df[best_col].mean()

        # 目標値と平均を%で表示（CVRなど）
        if unit == "%":
            avg_goal_display = f"{avg_goal * 100:.2f}%"
            mean_val_display = f"{mean_val * 100:.2f}%"
        else:
            avg_goal_display = f"{avg_goal:,.0f}{unit}"
            mean_val_display = f"{mean_val:,.0f}{unit}"

        st.markdown(f"""
        <div class="summary-card">
            <div class="card">🎯 目標値<br><div class="value">{avg_goal_display}</div></div>
            <div class="card">💎 ハイ達成<br><div class="value">{count_high}件</div></div>
            <div class="card">🟢 通常達成<br><div class="value">{count_good}件</div></div>
            <div class="card">🟡 もう少し<br><div class="value">{count_mid}件</div></div>
            <div class="card">✖️ 未達成<br><div class="value">{count_ng}件</div></div>
            <div class="card">📈 平均<br><div class="value">{mean_val_display}</div></div>
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
            custom_data=[plot_df[metric]]
        )
        fig.update_traces(
            textposition="outside", marker_line_width=0, width=0.25,
            hovertemplate=(
                "<b>%{y}</b><br>実績値: %{customdata[0]:.2%}" if unit == "%" else "<b>%{y}</b><br>実績値: %{customdata[0]:,.0f}" + unit
            ) + "<br>達成率: %{x:.1f}%<extra></extra>",
            textfont_size=14
        )
        fig.update_layout(
            xaxis_title="達成率（%）", yaxis_title="", showlegend=True,
            height=200 + len(plot_df) * 40, width=1000,
            margin=dict(t=40, l=60, r=20), modebar=dict(remove=True),
            font=dict(size=14)
        )
        st.plotly_chart(fig, use_container_width=False)
