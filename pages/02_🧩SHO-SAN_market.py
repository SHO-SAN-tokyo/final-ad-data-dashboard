# 02_🧩SHO-SAN_market.py   ★ClientSettings を結合して focus_level フィルタを追加
import streamlit as st
import pandas as pd, numpy as np, plotly.express as px
from google.cloud import bigquery

# ------------------------------------------------------------
# 0. ページ設定
# ------------------------------------------------------------
st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧩 SHO‑SAN market")
st.subheader("📊 カテゴリ × 都道府県  キャンペーン達成率モニター")

# ------------------------------------------------------------
# 1. BigQuery から 3 テーブル読込（Final_Ad_Data / KPI / ClientSettings）
# ------------------------------------------------------------
bq_cred = dict(st.secrets["connections"]["bigquery"])
bq_cred["private_key"] = bq_cred["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(bq_cred)

@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """必要テーブルをすべて取得して結合まで済ませて返す"""
    # ── メイン数値
    df = client.query("""
        SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
    """).to_dataframe()

    # ── KPI マスタ
    kpi_df = client.query("""
        SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta
    """).to_dataframe()

    # ── クライアント設定（focus_level 取得用）★追加
    settings = client.query("""
        SELECT client_name, focus_level
        FROM careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings
    """).to_dataframe()

    # Final_Ad_Data と結合（client_name キー）
    df = df.merge(settings, on="client_name", how="left")

    return df, kpi_df

df, kpi_df = load_data()

# ------------------------------------------------------------
# 2. 前処理
# ------------------------------------------------------------
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
for col in ["Cost", "Clicks", "Impressions", "コンバージョン数"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ------------------------------------------------------------
# 3. 日付フィルタ
# ------------------------------------------------------------
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
sel = st.date_input("📅 期間を選択", (dmin, dmax), min_value=dmin, max_value=dmax)
if isinstance(sel, (list, tuple)) and len(sel) == 2:
    s, e = map(pd.to_datetime, sel)
    df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

# ------------------------------------------------------------
# 4. 集計前に最新 CV を取り出しておく
# ------------------------------------------------------------
latest_cv = (
    df.sort_values("Date")
      .dropna(subset=["Date"])
      .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax(),
           ["CampaignId", "コンバージョン数"]]
      .rename(columns={"コンバージョン数": "最新CV"})
)

# ------------------------------------------------------------
# 5. 集計（focus_level を保持するため first で集約）★focus_level 追加
# ------------------------------------------------------------
agg = (
    df.groupby("CampaignId")
      .agg({
          "Cost": "sum",
          "Clicks": "sum",
          "Impressions": "sum",
          "カテゴリ": "first",
          "広告目的": "first",
          "都道府県": "first",
          "地方": "first",
          "CampaignName": "first",
          "focus_level": "first"          # 追加
      })
      .reset_index()
)

merged = agg.merge(latest_cv, on="CampaignId", how="left")
merged["CTR"] = merged["Clicks"] / merged["Impressions"].replace(0, np.nan)
merged["CVR"] = merged["最新CV"] / merged["Clicks"].replace(0, np.nan)
merged["CPA"] = merged["Cost"]   / merged["最新CV"].replace(0, np.nan)
merged["CPC"] = merged["Cost"]   / merged["Clicks"].replace(0, np.nan)
merged["CPM"] = merged["Cost"] * 1000 / merged["Impressions"].replace(0, np.nan)

# ------------------------------------------------------------
# 6. KPI マスタを数値化して結合
# ------------------------------------------------------------
goal_cols = [f"{m}_{l}" for m in ["CPA", "CVR", "CTR", "CPC", "CPM"]
                         for l in ["best", "good", "min"]]
kpi_df[goal_cols] = kpi_df[goal_cols].apply(pd.to_numeric, errors="coerce")
merged = merged.merge(kpi_df, on=["カテゴリ", "広告目的"], how="left")

# ------------------------------------------------------------
# 7. サイドバー フィルタ（focus_level を最上段に追加）★追加
# ------------------------------------------------------------
c0, c1, c2, c3, c4 = st.columns(5)

with c0:
    foc_opts = ["すべて"] + sorted(merged["focus_level"].dropna().unique())
    foc_sel  = st.selectbox("フォーカスレベル", foc_opts)

with c1:
    cat_sel = st.selectbox("カテゴリ", ["すべて"] + sorted(merged["カテゴリ"].dropna().unique()))
with c2:
    obj_sel = st.selectbox("広告目的", ["すべて"] + sorted(merged["広告目的"].dropna().unique()))
with c3:
    reg_sel = st.selectbox("地方", ["すべて"] + sorted(merged["地方"].dropna().unique()))
with c4:
    pref_opts = merged["都道府県"].dropna().unique() if reg_sel == "すべて" \
                else merged[merged["地方"] == reg_sel]["都道府県"].dropna().unique()
    pref_sel = st.selectbox("都道府県", ["すべて"] + sorted(pref_opts))

# --- 選択値に従って絞り込み
if foc_sel != "すべて": merged = merged[merged["focus_level"] == foc_sel]
if cat_sel != "すべて": merged = merged[merged["カテゴリ"]    == cat_sel]
if obj_sel != "すべて": merged = merged[merged["広告目的"]    == obj_sel]
if reg_sel != "すべて": merged = merged[merged["地方"]       == reg_sel]
if pref_sel != "すべて": merged = merged[merged["都道府県"]   == pref_sel]

# ------------------------------------------------------------
# 8. CSS（変更なし）
# ------------------------------------------------------------
st.markdown("""
<style>
div[role="tab"]>p{padding:0 20px;}
section[data-testid="stHorizontalBlock"]>div{padding:0 80px;justify-content:center!important;}
section[data-testid="stHorizontalBlock"] div[role="tab"]{
  min-width:180px!important;padding:.6rem 1.2rem;font-size:1.1rem;justify-content:center;}
.summary-card{display:flex;gap:2rem;margin:1rem 0 2rem;}
.card{background:#f8f9fa;padding:1rem 1.5rem;border-radius:.75rem;
      box-shadow:0 2px 5px rgba(0,0,0,.05);font-weight:bold;font-size:1.1rem;text-align:center;}
.card .value{font-size:1.5rem;margin-top:.5rem;}
</style>
""", unsafe_allow_html=True)

# ---------- ユーティリティ（% 補正はそのまま） -------------
def normalize_pct(s: pd.Series) -> pd.Series:
    return s / 100 if s.max() > 0.2 else s

def to_pct(v: float):
    if pd.isna(v): return np.nan
    return v * 100 if v < 1 else v
# ------------------------------------------------------------

# 9. 指標タブ（既存ロジックそのまま） -------------------------
tabs = st.tabs(["💰 CPA", "🔥 CVR", "⚡ CTR", "🧰 CPC", "📱 CPM"])
tab_map = {
    "💰 CPA": ("CPA", "CPA_best", "CPA_good", "CPA_min", "円", False, False),
    "🔥 CVR": ("CVR", "CVR_best", "CVR_good", "CVR_min", "%",  True,  True ),
    "⚡ CTR": ("CTR", "CTR_best", "CTR_good", "CTR_min", "%",  True,  True ),
    "🧰 CPC": ("CPC", "CPC_best", "CPC_good", "CPC_min", "円", False, False),
    "📱 CPM": ("CPM", "CPM_best", "CPM_good", "CPM_min", "円", False, False)
}
color_map = {
    "◎": "#57afbb",   # 爽やかな青
    "○": "#88c999",   # グリーン
    "△": "#d3dc74",   # 黄味の強いグリーン
    "×": "#e88c8c"    # やさしい赤
}

for lbl, (met, best, good, minv, unit, is_pct, big) in tab_map.items():
    with tabs[list(tab_map).index(lbl)]:
        st.markdown(f"### {lbl} 達成率グラフ")

        if is_pct:
            for col in (best, good, minv):
                merged[col] = normalize_pct(merged[col])

        merged["達成率"] = (merged[met] / merged[best]) * 100 if big \
                           else (merged[best] / merged[met]) * 100

        def judge(r):
            v, b, g, m = r[met], r[best], r[good], r[minv]
            if pd.isna(v) or pd.isna(b) or pd.isna(m): return None
            if (big and v >= b) or (not big and v <= b): return "◎"
            if (big and v >= g) or (not big and v <= g): return "○"
            if (big and v >= m) or (not big and v <= m): return "△"
            return "×"
        merged["評価"] = merged.apply(judge, axis=1)

        plot_df = merged[["都道府県", met, best, good, minv,
                          "CampaignName", "達成率", "評価"]].dropna()
        plot_df = plot_df[plot_df["都道府県"] != ""]
        if plot_df.empty():
            st.warning("📭 データがありません"); continue

        goal_val = plot_df[best].mean()
        mean_val = plot_df[met].mean()
        goal_val_disp = to_pct(goal_val) if is_pct else goal_val
        mean_val_disp = to_pct(mean_val) if is_pct else mean_val

        fmt = (lambda v: f"{v:,.0f}{unit}") if not is_pct else (lambda v: f"{v:.2f}{unit}")
        cnt = lambda s: (plot_df["評価"] == s).sum()

        st.markdown(f"""
        <div class="summary-card">
          <div class="card">🎯 目標値<div class="value">{fmt(goal_val_disp)}</div></div>
          <div class="card">💎 ハイ達成<div class="value">{cnt('◎')}件</div></div>
          <div class="card">🟢 通常達成<div class="value">{cnt('○')}件</div></div>
          <div class="card">🟡 もう少し<div class="value">{cnt('△')}件</div></div>
          <div class="card">✖️ 未達成<div class="value">{cnt('×')}件</div></div>
          <div class="card">📈 平均<div class="value">{fmt(mean_val_disp)}</div></div>
        </div>
        """, unsafe_allow_html=True)

        plot_df["達成率_pct"] = plot_df["達成率"].apply(to_pct)
        tool_val = plot_df[met].apply(to_pct) if is_pct else plot_df[met]
        tool_fmt = ":,.2f" if is_pct else ":,.0f"

        plot_df["ラベル"] = plot_df["CampaignName"].fillna("無名")
        fig = px.bar(
            plot_df,
            y="ラベル",
            x="達成率_pct",
            color="評価",
            orientation="h",
            color_discrete_map=color_map,
            text=plot_df["達成率_pct"].map(lambda x: f"{x:.1f}%"),
            custom_data=[tool_val.round(2 if is_pct else 0)]
        )
        fig.update_traces(
            textposition="outside",
            marker_line_width=0,
            width=.25,
            hovertemplate=(
                "<b>%{y}</b><br>実績値: %{customdata[0]"
                + tool_fmt + "}" + unit +
                "<br>達成率: %{x:.1f}%<extra></extra>"
            ),
            textfont_size=14
        )
        fig.update_layout(
            xaxis_title="達成率（%）",
            yaxis_title="",
            showlegend=True,
            xaxis_range=[0, max(plot_df["達成率_pct"].max() * 1.2, 1)],
            height=220 + len(plot_df) * 40,
            width=1000,
            margin=dict(t=40, l=60, r=20),
            modebar=dict(remove=True),
            font=dict(size=14)
        )
        st.plotly_chart(fig, use_container_width=False)
