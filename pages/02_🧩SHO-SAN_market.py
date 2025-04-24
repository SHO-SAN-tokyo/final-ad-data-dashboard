# 1_Main_Dashboard.py   ★今回の修正は “★ 修正” コメントだけ
import streamlit as st
import pandas as pd, numpy as np, re
import plotly.express as px
from google.cloud import bigquery

# ---------- 0. ページ設定 & CSS ----------
st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧩SHO-SAN market")
st.subheader("📊 カテゴリ × 都道府県  キャンペーン達成率モニター")

st.markdown(
    """
    <style>
      div[role="tab"] > p { padding: 0 20px; }
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
      .summary-card { display: flex; gap: 2rem; margin: 1rem 0 2rem 0; }
      .card {
          background: #f8f9fa;
          padding: 1rem 1.5rem;
          border-radius: 0.75rem;
          box-shadow: 0 2px 5px rgba(0,0,0,0.05);
          font-weight: bold; font-size: 1.1rem;
          text-align: center;
      }
      .card .value { font-size: 1.5rem; margin-top: 0.5rem; }
    </style>
    """,
    unsafe_allow_html=True
)

# ---------- 1. BigQuery ----------
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data
def load_data():
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data").to_dataframe()
    kpi_df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta").to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# ---------- 2. 前処理 ----------
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
for c in ["Cost", "Clicks", "Impressions", "コンバージョン数"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

# ---------- 3. 日付フィルター ----------
min_date, max_date = df["Date"].min().date(), df["Date"].max().date()
start_d, end_d = st.date_input("📅 期間を選択", (min_date, max_date),
                               min_value=min_date, max_value=max_date)
df = df[(df["Date"].dt.date >= start_d) & (df["Date"].dt.date <= end_d)]

# ---------- 4. 最新 CV を抽出 ----------
latest_cv = (df.sort_values("Date")
               .dropna(subset=["Date"])
               .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax()]
               [["CampaignId", "コンバージョン数"]]
               .rename(columns={"コンバージョン数": "最新CV"}))

# ---------- 5. 集計 ----------
agg = (df.groupby("CampaignId")
         .agg({"Cost":"sum","Clicks":"sum","Impressions":"sum",
               "カテゴリ":"first","広告目的":"first","都道府県":"first","地方":"first","CampaignName":"first"})
         .reset_index())

merged = (agg.merge(latest_cv, on="CampaignId", how="left")
              .assign(CTR=lambda d: d["Clicks"]/d["Impressions"],
                      CVR=lambda d: d["最新CV"]/d["Clicks"],
                      CPA=lambda d: d["Cost"]/d["最新CV"],
                      CPC=lambda d: d["Cost"]/d["Clicks"],
                      CPM=lambda d: d["Cost"]/d["Impressions"]*1000))

# ---------- 6. KPI を付与 ----------
goal_cols = [f"{kpi}_{lvl}" for kpi in ["CPA","CVR","CTR","CPC","CPM"]
                          for lvl in ["best","good","min"]]
for col in goal_cols:
    if col in kpi_df.columns:
        kpi_df[col]=pd.to_numeric(kpi_df[col], errors="coerce")
merged = pd.merge(merged, kpi_df, on=["カテゴリ","広告目的"], how="left")

# ---------- 7. 条件フィルター ----------
st.sidebar.header("📂 条件を絞り込む")
col1,col2,col3,col4 = st.columns(4)
with col1:
    f_cat = st.selectbox("カテゴリ", ["すべて"]+sorted(merged["カテゴリ"].dropna().unique()))
with col2:
    f_obj = st.selectbox("広告目的", ["すべて"]+sorted(merged["広告目的"].dropna().unique()))
with col3:
    f_reg = st.selectbox("地方", ["すべて"]+sorted(merged["地方"].dropna().unique()))
with col4:
    cand_pref = (merged[merged["地方"]==f_reg]["都道府県"].dropna().unique()
                 if f_reg!="すべて" else merged["都道府県"].dropna().unique())
    f_pref = st.selectbox("都道府県", ["すべて"]+sorted(cand_pref))

if f_cat  != "すべて": merged = merged[merged["カテゴリ"]==f_cat]
if f_obj  != "すべて": merged = merged[merged["広告目的"]==f_obj]
if f_reg  != "すべて": merged = merged[merged["地方"]==f_reg]
if f_pref != "すべて": merged = merged[merged["都道府県"]==f_pref]

# ---------- 8. 指標タブ ----------
tabs = st.tabs(["💰 CPA","🔥 CVR","⚡ CTR","🧰 CPC","📱 CPM"])
tab_map = {
    "💰 CPA": ("CPA","CPA_best","CPA_good","CPA_min","円"),
    "🔥 CVR": ("CVR","CVR_best","CVR_good","CVR_min","%"),
    "⚡ CTR": ("CTR","CTR_best","CTR_good","CTR_min","%"),
    "🧰 CPC": ("CPC","CPC_best","CPC_good","CPC_min","円"),
    "📱 CPM": ("CPM","CPM_best","CPM_good","CPM_min","円")
}
color_map = {"◎":"#88c999","○":"#d3dc74","△":"#f3b77d","×":"#e88c8c"}

for lab,(met,best,good,minv,unit) in tab_map.items():
    with tabs[list(tab_map.keys()).index(lab)]:
        st.markdown(f"### {lab} 達成率グラフ")

        # ------------------------ ここから下を CVR だけ調整 ------------------------
        merged["達成率"] = merged[best] / merged[met] * 100

        # 判定
        def judge(row):
            v=row[met]
            if pd.isna(v) or pd.isna(row[minv]): return None
            if v<=row[best]: return "◎"
            elif v<=row[good]: return "○"
            elif v<=row[minv]: return "△"
            else: return "×"
        merged["評価"]=merged.apply(judge,axis=1)

        plot_df = merged[["都道府県",met,best,good,minv,
                          "CampaignName","達成率","評価"]].dropna()
        plot_df = plot_df[plot_df["都道府県"]!=""]

        if plot_df.empty:
            st.warning("📭 データがありません")
            continue

        # ------ サマリーカード ------
        cnt = lambda sym: (plot_df["評価"]==sym).sum()
        mean_val = plot_df[met].mean()
        avg_goal = plot_df[best].mean()

        # ★ 修正: CVR/CTR は “%” 表示用に *100
        if lab=="🔥 CVR" or lab=="⚡ CTR":
            mean_val *= 100
            avg_goal *= 100

        st.markdown(f"""
        <div class="summary-card">
          <div class="card">🎯 目標値<br><div class="value">{avg_goal:.2f}{unit}</div></div>
          <div class="card">💎 ハイ達成<br><div class="value">{cnt('◎')}件</div></div>
          <div class="card">🟢 通常達成<br><div class="value">{cnt('○')}件</div></div>
          <div class="card">🟡 もう少し<br><div class="value">{cnt('△')}件</div></div>
          <div class="card">✖️ 未達成<br><div class="value">{cnt('×')}件</div></div>
          <div class="card">📈 平均<br><div class="value">{mean_val:.2f}{unit}</div></div>
        </div>
        """,unsafe_allow_html=True)

        # ------ グラフ ------
        # ★ 修正: hover 用の実績値は % 換算
        custom_display = plot_df[met]*100 if lab=="🔥 CVR" else plot_df[met]
        plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")

        fig = px.bar(
            plot_df, y="ラベル", x="達成率", color="評価", orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率"].map(lambda x:f"{x:.1f}%"),
            custom_data=[custom_display]
        )
        fig.update_traces(
            textposition="outside", marker_line_width=0, width=0.25,
            hovertemplate=(
                "<b>%{y}</b><br>実績値: %{customdata[0]:,.2f}"
                f"{unit}<br>達成率: "+"%{x:.1f}%<extra></extra>"),
            textfont_size=14
        )
        fig.update_layout(
            xaxis_title="達成率（%）", yaxis_title="", showlegend=True,
            height=200+len(plot_df)*40, width=1000,
            margin=dict(t=40,l=60,r=20), modebar=dict(remove=True),
            font=dict(size=14)
        )
        st.plotly_chart(fig, use_container_width=False)
