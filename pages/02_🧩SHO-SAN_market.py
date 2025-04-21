import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery

st.set_page_config(page_title="SHO-SAN Market - 達成率分析", layout="wide")
st.title("📊 カテゴリ×都道府県 達成率モニター")

# --- BigQuery 認証 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# --- データ読み込み ---
@st.cache_data
def load_data():
    df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`").to_dataframe()
    kpi_df = client.query("SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta`").to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# --- 前処理 ---
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# 最新CV（キャンペーンごとに1件）
latest_cv = df.sort_values("Date").dropna(subset=["Date"])
latest_cv = latest_cv.loc[latest_cv.groupby("CampaignId")["Date"].idxmax()]
latest_cv = latest_cv[["CampaignId", "コンバージョン数"]].rename(columns={"コンバージョン数": "最新CV"})

# 集計（キャンペーンごと）
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

# KPIと結合
merged = pd.merge(merged, kpi_df, on=["カテゴリ", "広告目的"], how="left")

# --- 評価関数 ---
def get_evaluation(value, best, good, min_, reverse=False):
    if pd.isna(value) or pd.isna(best) or pd.isna(good) or pd.isna(min_):
        return None
    if reverse:
        if value <= best:
            return "◎"
        elif value <= good:
            return "○"
        elif value <= min_:
            return "△"
        else:
            return "×"
    else:
        if value >= best:
            return "◎"
        elif value >= good:
            return "○"
        elif value >= min_:
            return "△"
        else:
            return "×"

# --- カテゴリ選択 ---
selected_category = st.selectbox("📂 表示カテゴリ", sorted(merged["カテゴリ"].unique()))
cat_df = merged[merged["カテゴリ"] == selected_category]

# --- 指標別タブ ---
tabs = st.tabs(["🎯 CPA", "🔁 CVR", "⚡ CTR", "🧮 CPC", "📡 CPM"])
tab_map = {
    "🎯 CPA": ("CPA", "CPA_best", "CPA_good", "CPA_min", True),
    "🔁 CVR": ("CVR", "CVR_best", "CVR_good", "CVR_min", False),
    "⚡ CTR": ("CTR", "CTR_best", "CTR_good", "CTR_min", False),
    "🧮 CPC": ("CPC", "CPC_best", "CPC_good", "CPC_min", True),
    "📡 CPM": ("CPM", "CPM_best", "CPM_good", "CPM_min", True),
}

for label, (metric, best_col, good_col, min_col, reverse) in tab_map.items():
    with tabs[list(tab_map.keys()).index(label)]:
        st.subheader(f"{label} 達成率グラフ")

        plot_df = cat_df[["都道府県", metric, best_col, good_col, min_col]].copy()
        plot_df = plot_df.dropna(subset=[metric, best_col, good_col, min_col])
        plot_df = plot_df[plot_df["都道府県"] != ""]
        plot_df = plot_df[plot_df[metric] > 0]

        if plot_df.empty:
            st.info("📭 データがありません（目標未設定または数値不足）")
            continue

        # 達成率と評価
        plot_df["達成率"] = (plot_df[best_col] / plot_df[metric]) * 100 if not reverse else (plot_df[metric] / plot_df[best_col]) * 100
        plot_df["評価"] = plot_df.apply(
            lambda row: get_evaluation(row[metric], row[best_col], row[good_col], row[min_col], reverse), axis=1
        )
        plot_df = plot_df.sort_values("達成率", ascending=False)

        # 色分け（達成：緑、未達：赤、その他：グレー）
        colors = ["green" if ev == "◎" else "red" if ev == "×" else "#999999" for ev in plot_df["評価"]]

        # グラフ描画
        fig, ax = plt.subplots(figsize=(8, max(4, len(plot_df)*0.4)))
        bars = ax.barh(plot_df["都道府県"], plot_df["達成率"], color=colors)
        ax.set_xlabel("達成率（%）")
        ax.set_title(f"{selected_category}｜{metric}達成率")
        ax.set_xlim(0, max(120, plot_df["達成率"].max() + 10))

        for bar, val, ev in zip(bars, plot_df["達成率"], plot_df["評価"]):
            ax.text(val + 1, bar.get_y() + bar.get_height()/2, f"{val:.1f}% {ev}", va='center')

        goal_val = plot_df[best_col].mean()
        ax.text(0.98, 1.03, f"🎯 平均目標（best）：{goal_val:.2f}", transform=ax.transAxes,
                ha="right", va="bottom", fontsize=10, fontweight="bold", color="gray")

        st.pyplot(fig)
