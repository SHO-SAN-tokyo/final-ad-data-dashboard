# 02_🧩SHO-SAN_market.py   ★表示フォーマットだけ微調整
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
    df     = client.query(
        "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
    ).to_dataframe()
    kpi_df = client.query(
        "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta"
    ).to_dataframe()
    return df, kpi_df

df, kpi_df = load_data()

# ------------------------------------------------------------
# 2. 前処理
# ------------------------------------------------------------
df["Date"]        = pd.to_datetime(df["Date"], errors="coerce")
for col in ["Cost","Clicks","Impressions","コンバージョン数"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ------------------------------------------------------------
# 3. 日付フィルタ
# ------------------------------------------------------------
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
sel = st.date_input("📅 期間を選択", (dmin, dmax), min_value=dmin, max_value=dmax)
if isinstance(sel, (list, tuple)) and len(sel) == 2:
    s, e = map(pd.to_datetime, sel)
    df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

# 最新 CV を CampaignId 単位で取得
latest_cv = (df.sort_values("Date").dropna(subset=["Date"])
               .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax(),
                    ["CampaignId","コンバージョン数"]]
               .rename(columns={"コンバージョン数":"最新CV"}))

# 基礎集計
agg = (df.groupby("CampaignId")
         .agg({"Cost":"sum","Clicks":"sum","Impressions":"sum",
               "カテゴリ":"first","広告目的":"first","都道府県":"first",
               "地方":"first","CampaignName":"first"})
         .reset_index())

merged = agg.merge(latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"]       / merged["Impressions"].replace(0, np.nan)
merged["CVR"] = merged["最新CV"]       / merged["Clicks"].replace(0, np.nan)
merged["CPA"] = merged["Cost"]         / merged["最新CV"].replace(0, np.nan)
merged["CPC"] = merged["Cost"]         / merged["Clicks"].replace(0, np.nan)
merged["CPM"] = merged["Cost"]*1000    / merged["Impressions"].replace(0, np.nan)

# KPI テーブル数値化
goal_cols = [f"{m}_{lvl}" for m in ["CPA","CVR","CTR","CPC","CPM"]
                         for lvl in ["best","good","min"]]
kpi_df[goal_cols] = kpi_df[goal_cols].apply(pd.to_numeric, errors="coerce")
merged = merged.merge(kpi_df, on=["カテゴリ","広告目的"], how="left")

# ------------------------------------------------------------
# 4. 条件フィルタ
# ------------------------------------------------------------
c1,c2,c3,c4 = st.columns(4)
with c1:
    cat_sel = st.selectbox("カテゴリ", ["すべて"]+sorted(merged["カテゴリ"].dropna().unique()))
with c2:
    obj_sel = st.selectbox("広告目的", ["すべて"]+sorted(merged["広告目的"].dropna().unique()))
with c3:
    reg_sel = st.selectbox("地方", ["すべて"]+sorted(merged["地方"].dropna().unique()))
with c4:
    pref_opts = merged["都道府県"].dropna().unique() if reg_sel=="すべて" \
                else merged[merged["地方"]==reg_sel]["都道府県"].dropna().unique()
    pref_sel = st.selectbox("都道府県", ["すべて"]+sorted(pref_opts))

if cat_sel!="すべて":  merged = merged[merged["カテゴリ"]   == cat_sel]
if obj_sel!="すべて":  merged = merged[merged["広告目的"] == obj_sel]
if reg_sel!="すべて":  merged = merged[merged["地方"]     == reg_sel]
if pref_sel!="すべて": merged = merged[merged["都道府県"] == pref_sel]

# ------------------------------------------------------------
# 5. CSS
# ------------------------------------------------------------
st.markdown("""<style>
div[role="tab"] > p {padding:0 20px;}
section[data-testid="stHorizontalBlock"] > div{padding:0 80px;justify-content:center!important;}
section[data-testid="stHorizontalBlock"] div[role="tab"]{
  min-width:180px !important;padding:.6rem 1.2rem;font-size:1.1rem;justify-content:center;}
.summary-card{display:flex;gap:2rem;margin:1rem 0 2rem;}
.card{background:#f8f9fa;padding:1rem 1.5rem;border-radius:.75rem;
      box-shadow:0 2px 5px rgba(0,0,0,.05);font-weight:bold;font-size:1.1rem;text-align:center;}
.card .value{font-size:1.5rem;margin-top:.5rem;}
</style>""", unsafe_allow_html=True)

# ------------------------------------------------------------
# 6. 指標タブ
# ------------------------------------------------------------
tabs = st.tabs(["💰 CPA","🔥 CVR","⚡ CTR","🧰 CPC","📱 CPM"])
tab_map = {
    "💰 CPA":("CPA","CPA_best","CPA_good","CPA_min","円", False),
    "🔥 CVR":("CVR","CVR_best","CVR_good","CVR_min","%", True),
    "⚡ CTR":("CTR","CTR_best","CTR_good","CTR_min","%", True),
    "🧰 CPC":("CPC","CPC_best","CPC_good","CPC_min","円", False),
    "📱 CPM":("CPM","CPM_best","CPM_good","CPM_min","円", False)
}
color_map = {"◎":"#88c999","○":"#d3dc74","△":"#f3b77d","×":"#e88c8c"}

for label,(met,best,good,minv,unit,is_pct) in tab_map.items():
    with tabs[list(tab_map.keys()).index(label)]:
        st.markdown(f"### {label} 達成率グラフ")

        merged["達成率"] = merged[best].div(merged[met]) * 100

        def judge(r):
            v = r[met]
            if pd.isna(v) or pd.isna(r[minv]): return None
            if v <= r[best]:   return "◎"
            elif v <= r[good]: return "○"
            elif v <= r[minv]: return "△"
            else:              return "×"
        merged["評価"] = merged.apply(judge, axis=1)

        plot_df = merged[["都道府県",met,best,good,minv,
                          "CampaignName","達成率","評価"]].dropna()
        plot_df = plot_df[plot_df["都道府県"]!=""]

        if plot_df.empty:
            st.warning("📭 データがありません"); continue

        # --- ★ 目標値と平均値の表示を調整 ---------------------------
        goal_val = plot_df[best].mean()
        mean_val = plot_df[met].mean()

        # 目標・平均とも 1 以上なら %換算と思われる → 0-100 を 0-1 に戻す
        if is_pct:
            if goal_val >= 1:  goal_val /= 100
            if mean_val >= 1:  mean_val /= 100

        fmt_num   = lambda v: f"{v:,.0f}{unit}"
        fmt_pct   = lambda v: f"{v*100:.2f}{unit}"
        fmt_value = fmt_pct if is_pct else fmt_num

        cnt = lambda s: (plot_df["評価"]==s).sum()
        s_card = f"""
        <div class="summary-card">
          <div class="card">🎯 目標値<div class="value">{fmt_value(goal_val)}</div></div>
          <div class="card">💎 ハイ達成<div class="value">{cnt('◎')}件</div></div>
          <div class="card">🟢 通常達成<div class="value">{cnt('○')}件</div></div>
          <div class="card">🟡 もう少し<div class="value">{cnt('△')}件</div></div>
          <div class="card">✖️ 未達成<div class="value">{cnt('×')}件</div></div>
          <div class="card">📈 平均<div class="value">{fmt_value(mean_val)}</div></div>
        </div>"""
        st.markdown(s_card, unsafe_allow_html=True)
        # ------------------------------------------------------------

        # ツールチップ用に実績値をフォーマット（CPA は整数、% は 2 桁）
        tooltip_val = plot_df[met].copy()
        if is_pct:
            tooltip_val = tooltip_val.apply(lambda x: x*100 if x<1 else x)
            tooltip_fmt = ":,.2f"
        else:
            tooltip_fmt = ":,.0f"

        plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")
        fig = px.bar(
            plot_df,
            y="ラベル", x="達成率",
            color="評価", orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率"].map(lambda x:f"{x:.1f}%"),
            custom_data=[tooltip_val]
        )
        fig.update_traces(
            textposition="outside", marker_line_width=0, width=.25,
            hovertemplate=(
               "<b>%{y}</b><br>実績値: %{customdata[0]"+tooltip_fmt+"}"+unit+
               "<br>達成率: %{x:.1f}%<extra></extra>"
            ),
            textfont_size=14
        )
        fig.update_layout(
            xaxis_title="達成率（%）", yaxis_title="", showlegend=True,
            height=220+len(plot_df)*40, width=1000,
            margin=dict(t=40,l=60,r=20), modebar=dict(remove=True),
            font=dict(size=14)
        )
        st.plotly_chart(fig, use_container_width=False)
