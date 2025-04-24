# 02_🧩SHO-SAN_market.py   （全文）★今回の修正は “★ 修正” だけ

import streamlit as st
import pandas as pd, numpy as np, plotly.express as px, re
from google.cloud import bigquery

# ------------------------------------------------------------
# 0. ページ設定
# ------------------------------------------------------------
st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧩SHO-SAN market")
st.subheader("📊 カテゴリ × 都道府県  キャンペーン達成率モニター")

# ------------------------------------------------------------
# 1. BigQuery 読み込み
# ------------------------------------------------------------
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

@st.cache_data(show_spinner=False)
def load_data():
    df      = client.query(
        "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
    ).to_dataframe()
    kpi_df  = client.query(
        "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta"
    ).to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# ------------------------------------------------------------
# 2. 前処理
# ------------------------------------------------------------
df["Date"]           = pd.to_datetime(df["Date"],            errors="coerce")
df["Cost"]           = pd.to_numeric(df["Cost"],             errors="coerce").fillna(0)
df["Clicks"]         = pd.to_numeric(df["Clicks"],           errors="coerce").fillna(0)
df["Impressions"]    = pd.to_numeric(df["Impressions"],      errors="coerce").fillna(0)
df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# 3. 日付フィルター
# ------------------------------------------------------------
st.markdown("<h5 style='margin-top:2rem;'>📅 日付フィルター</h5>",
            unsafe_allow_html=True)
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
sel_date = st.date_input("期間を選択", (dmin, dmax), min_value=dmin, max_value=dmax)
if isinstance(sel_date, (list, tuple)) and len(sel_date) == 2:
    s, e = map(pd.to_datetime, sel_date)
    df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

# 最新 CV 数だけ取得
latest_cv = (df.sort_values("Date")
               .dropna(subset=["Date"])
               .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax(), ["CampaignId","コンバージョン数"]]
               .rename(columns={"コンバージョン数":"最新CV"}))

# 集計
agg = (df.groupby("CampaignId")
         .agg({"Cost":"sum","Clicks":"sum","Impressions":"sum",
               "カテゴリ":"first","広告目的":"first","都道府県":"first",
               "地方":"first","CampaignName":"first"})
         .reset_index())

merged = agg.merge(latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"].div(merged["Impressions"].replace(0, np.nan))
merged["CVR"] = merged["最新CV"].div(merged["Clicks"].replace(0, np.nan))
merged["CPA"] = merged["Cost"].div(merged["最新CV"].replace(0, np.nan))
merged["CPC"] = merged["Cost"].div(merged["Clicks"].replace(0, np.nan))
merged["CPM"] = merged["Cost"].div(merged["Impressions"].replace(0, np.nan))*1000

# KPI テーブルを数値化してマージ
goal_cols = [f"{kpi}_{lvl}" for kpi in ["CPA","CVR","CTR","CPC","CPM"]
                            for lvl in ["best","good","min"]]
kpi_df[goal_cols] = kpi_df[goal_cols].apply(pd.to_numeric, errors="coerce")
merged = merged.merge(kpi_df, on=["カテゴリ","広告目的"], how="left")

# ------------------------------------------------------------
# 4. 条件フィルター
# ------------------------------------------------------------
st.markdown("<h5 style='margin-top:2rem;'>📂 条件を絞り込む</h5>", unsafe_allow_html=True)
c1,c2,c3,c4 = st.columns(4)
with c1:
    cat_sel   = st.selectbox("カテゴリ",    ["すべて"]+sorted(merged["カテゴリ"].dropna().unique()))
with c2:
    obj_sel   = st.selectbox("広告目的",    ["すべて"]+sorted(merged["広告目的"].dropna().unique()))
with c3:
    reg_sel   = st.selectbox("地方",        ["すべて"]+sorted(merged["地方"].dropna().unique()))
with c4:
    pref_opts = merged[merged["地方"]==reg_sel]["都道府県"].dropna().unique() \
                if reg_sel!="すべて" else merged["都道府県"].dropna().unique()
    pref_sel  = st.selectbox("都道府県", ["すべて"]+sorted(pref_opts))

if cat_sel  != "すべて": merged = merged[merged["カテゴリ"]   == cat_sel]
if obj_sel  != "すべて": merged = merged[merged["広告目的"] == obj_sel]
if reg_sel  != "すべて": merged = merged[merged["地方"]     == reg_sel]
if pref_sel != "すべて": merged = merged[merged["都道府県"] == pref_sel]

# ------------------------------------------------------------
# 5. CSS for Tabs & Cards
# ------------------------------------------------------------
st.markdown("""
<style>
div[role="tab"] > p { padding:0 20px; }
section[data-testid="stHorizontalBlock"] > div { padding:0 80px; justify-content:center !important; }
section[data-testid="stHorizontalBlock"] div[role="tab"]{
  min-width:180px !important;padding:.6rem 1.2rem;font-size:1.1rem;justify-content:center;}
.summary-card{display:flex;gap:2rem;margin:1rem 0 2rem;}
.card{background:#f8f9fa;padding:1rem 1.5rem;border-radius:.75rem;
      box-shadow:0 2px 5px rgba(0,0,0,.05);font-weight:bold;font-size:1.1rem;text-align:center;}
.card .value{font-size:1.5rem;margin-top:.5rem;}
</style>""", unsafe_allow_html=True)

# ------------------------------------------------------------
# 6. タブ表示
# ------------------------------------------------------------
tabs = st.tabs(["💰 CPA","🔥 CVR","⚡ CTR","🧰 CPC","📱 CPM"])
tab_map = {
    "💰 CPA":("CPA","CPA_best","CPA_good","CPA_min","円"),
    "🔥 CVR":("CVR","CVR_best","CVR_good","CVR_min","%"),
    "⚡ CTR":("CTR","CTR_best","CTR_good","CTR_min","%"),
    "🧰 CPC":("CPC","CPC_best","CPC_good","CPC_min","円"),
    "📱 CPM":("CPM","CPM_best","CPM_good","CPM_min","円")
}
color_map = {"◎":"#88c999","○":"#d3dc74","△":"#f3b77d","×":"#e88c8c"}

for tab_label, (met,best,good,minv,unit) in tab_map.items():
    with tabs[list(tab_map.keys()).index(tab_label)]:
        st.markdown(f"### {tab_label} 達成率グラフ")

        merged["達成率"] = (merged[best] / merged[met]) * 100

        def judge(row):
            val = row[met]
            if pd.isna(val) or pd.isna(row[minv]):   return None
            if val <= row[best]:                     return "◎"
            elif val <= row[good]:                   return "○"
            elif val <= row[minv]:                   return "△"
            else:                                   return "×"

        merged["評価"] = merged.apply(judge, axis=1)

        plot_df = merged[["都道府県",met,best,good,minv,
                          "CampaignName","達成率","評価"]].dropna()
        plot_df = plot_df[plot_df["都道府県"]!=""]

        # ★ 修正: empty 判定はプロパティで
        if plot_df.empty:
            st.warning("📭 データがありません")
            continue

        # 件数・平均
        cnt  = lambda s: (plot_df["評価"]==s).sum()
        mean_val = plot_df[met].mean()
        goal_val = plot_df[best].mean()

        # ★ 修正: CVR / CTR は % 換算（0.006 → 0.60 など）
        if unit == "%":
            mean_val = mean_val * 100
            goal_val = goal_val * 100

        st.markdown(f"""
        <div class="summary-card">
          <div class="card">🎯 目標値<div class="value">{goal_val:.2f}{unit}</div></div>
          <div class="card">💎 ハイ達成<div class="value">{cnt('◎')}件</div></div>
          <div class="card">🟢 通常達成<div class="value">{cnt('○')}件</div></div>
          <div class="card">🟡 もう少し<div class="value">{cnt('△')}件</div></div>
          <div class="card">✖️ 未達成<div class="value">{cnt('×')}件</div></div>
          <div class="card">📈 平均<div class="value">{mean_val:.2f}{unit}</div></div>
        </div>
        """, unsafe_allow_html=True)

        # 実績値ツールチップ用
        tooltip_val = plot_df[met] * (100 if unit=="%" else 1)

        plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")
        fig = px.bar(
            plot_df,
            y="ラベル", x="達成率", color="評価", orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率"].map(lambda x:f"{x:.1f}%"),
            custom_data=[tooltip_val]
        )
        fig.update_traces(
            textposition="outside", marker_line_width=0, width=.25,
            hovertemplate=("<b>%{y}</b><br>実績値: %{customdata[0]:,.2f}"
                           f"{unit}<br>達成率: %{x:.1f}%<extra></extra>"),
            textfont_size=14
        )
        fig.update_layout(
            xaxis_title="達成率（%）", yaxis_title="", showlegend=True,
            height=200+len(plot_df)*40, width=1000,
            margin=dict(t=40,l=60,r=20), modebar=dict(remove=True),
            font=dict(size=14)
        )
        st.plotly_chart(fig, use_container_width=False)
