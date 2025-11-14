import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery

from auth import require_login

# ──────────────────────────────────────────────
# ログイン & ページ共通設定
# ──────────────────────────────────────────────
require_login()
st.set_page_config(page_title="🧩 SHO-SAN market", layout="wide")

st.markdown(
    "<h1 style='display:inline-block;margin-bottom:0;'>🧩 SHO-SAN market ／全件</h1>",
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────
# BigQuery クライアント
# ──────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    cred = dict(st.secrets["connections"]["bigquery"])
    # 改行コードを復元（Ad Drive と同じ）
    cred["private_key"] = cred["private_key"].replace("\\n", "\n")
    return bigquery.Client.from_service_account_info(cred)

bq = get_bq_client()

# ──────────────────────────────────────────────
# データ取得
#   ※ Ad Drive と同じ Final_Ad_Data_Last をベースにする
# ──────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_final_ad_data():
    query = """
        SELECT *
        FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data_Last`
    """
    return bq.query(query).to_dataframe()


@st.cache_data(show_spinner=False)
def load_kpi_settings():
    query = """
        SELECT *
        FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta`
    """
    return bq.query(query).to_dataframe()


@st.cache_data
def load_cv_targets():
    query = """
    SELECT
      `キャンペーンID`,
      `配信月`,
      MAX(SAFE_CAST(`目標CPA` AS FLOAT64)) AS `目標CPA`
    FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.CV_List`
    WHERE SAFE_CAST(`目標CPA` AS FLOAT64) IS NOT NULL
    GROUP BY
      `キャンペーンID`,
      `配信月`
    """
    return bq.query(query).to_dataframe()


df_raw = load_final_ad_data()
df_kpi = load_kpi_settings()
df_cv_target = load_cv_targets()

if df_raw.empty:
    st.warning("Final_Ad_Data_Last にデータがありません。")
    st.stop()

# ──────────────────────────────────────────────
# 前処理（Ad Drive に揃える）
# ──────────────────────────────────────────────

# conv_total 列名を Ad Drive と合わせる
if "コンバージョン数" in df_raw.columns:
    df_raw = df_raw.rename(columns={"コンバージョン数": "conv_total"})

# 数値列を明示的に数値化
for col in ["Cost", "Clicks", "Impressions", "conv_total"]:
    if col in df_raw.columns:
        df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

# 配信月（文字列と datetime の両方を用意）
if "配信月" in df_raw.columns:
    df_raw["配信月"] = df_raw["配信月"].astype(str)
    # "YYYY/MM" でも "YYYY-MM" でもパースできるように一旦 - に統一
    df_raw["配信月_norm"] = (
        df_raw["配信月"]
        .str.replace(".", "-", regex=False)
        .str.replace("/", "-", regex=False)
    )
    df_raw["配信月_dt"] = pd.to_datetime(
        df_raw["配信月_norm"] + "-01",
        format="%Y-%m-%d",
        errors="coerce",
    )
    # 表示用は "YYYY/MM" 統一
    df_raw["配信月"] = df_raw["配信月_dt"].dt.strftime("%Y/%m")

# building_count が無いケースもありうるので補完
if "building_count" not in df_raw.columns:
    df_raw["building_count"] = "未設定"

# ──────────────────────────────────────────────
# キャンペーン単位にまとめて KPI マスタ & 目標CPA を付与
#   → Ad Drive と同じ考え方で集計
# ──────────────────────────────────────────────
group_cols = [
    "CampaignId",
    "キャンペーン名",
    "client_name",
    "building_count",
    "配信月",
    "広告媒体",
    "メインカテゴリ",
    "サブカテゴリ",
    "広告目的",
    "地方",
    "都道府県",
]
group_cols = [c for c in group_cols if c in df_raw.columns]

# CV は「その配信月の最新CV」を採用したいので max() にしておく
agg_dict = {
    "Cost": "sum",
    "Clicks": "sum",
    "Impressions": "sum",
    "conv_total": "max",
}

df_campaign = (
    df_raw
    .groupby(group_cols, dropna=False, as_index=False)
    .agg(agg_dict)
)

# === 指標算出（Ad Drive と同じ）★NA安全版 ===
for col in ["Cost", "Clicks", "Impressions", "conv_total"]:
    if col in df_campaign.columns:
        df_campaign[col] = pd.to_numeric(df_campaign[col], errors="coerce")

cost = df_campaign["Cost"]
clicks = df_campaign["Clicks"]
imps = df_campaign["Impressions"]
cv = df_campaign["conv_total"]

mask_cv_pos = (cv > 0).fillna(False)
mask_click_pos = (clicks > 0).fillna(False)
mask_imp_pos = (imps > 0).fillna(False)

df_campaign["CPA"] = np.where(mask_cv_pos, cost / cv, np.nan)
df_campaign["CVR"] = np.where(mask_click_pos, cv / clicks, np.nan)
df_campaign["CTR"] = np.where(mask_imp_pos, clicks / imps, np.nan)
df_campaign["CPC"] = np.where(mask_click_pos, cost / clicks, np.nan)
df_campaign["CPM"] = np.where(mask_imp_pos, cost * 1000.0 / imps, np.nan)

# KPI マスタを JOIN
if not df_kpi.empty:
    join_keys = ["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"]
    join_keys = [c for c in join_keys if c in df_campaign.columns and c in df_kpi.columns]
    if join_keys:
        df_campaign = df_campaign.merge(df_kpi, how="left", on=join_keys)

# CV_List から目標CPA を JOIN（CampaignId + 配信月）
if (
    not df_cv_target.empty
    and "CampaignId" in df_campaign.columns
    and "配信月" in df_campaign.columns
):
    df_campaign = df_campaign.merge(
        df_cv_target,
        how="left",
        left_on=["CampaignId", "配信月"],
        right_on=["キャンペーンID", "配信月"],
    )
    if "キャンペーンID" in df_campaign.columns:
        df_campaign = df_campaign.drop(columns=["キャンペーンID"])

# ──────────────────────────────────────────────
# 評価列（◎○△×）
# ──────────────────────────────────────────────
def grade_lower_better(val, best, good, min_):
    if pd.isna(val) or pd.isna(best) or pd.isna(good) or pd.isna(min_):
        return None
    if val <= best:
        return "◎"
    if val <= good:
        return "○"
    if val <= min_:
        return "△"
    return "×"


def grade_higher_better(val, best, good, min_):
    if pd.isna(val) or pd.isna(best) or pd.isna(good) or pd.isna(min_):
        return None
    if val >= best:
        return "◎"
    if val >= good:
        return "○"
    if val >= min_:
        return "△"
    return "×"


for metric, grader in [
    ("CPA", grade_lower_better),
    ("CPC", grade_lower_better),
    ("CPM", grade_lower_better),
    ("CVR", grade_higher_better),
    ("CTR", grade_higher_better),
]:
    base = metric
    df_campaign[f"{metric}_評価"] = df_campaign.apply(
        lambda r: grader(
            r.get(base),
            r.get(f"{base}_best"),
            r.get(f"{base}_good"),
            r.get(f"{base}_min"),
        ),
        axis=1,
    )

# ──────────────────────────────────────────────
# フィルター UI（Market 用）
# ──────────────────────────────────────────────
st.markdown("### 🔎 絞り込み条件")

def options(col: str):
    if col not in df_campaign.columns:
        return []
    vals = df_campaign[col].dropna().unique().tolist()
    vals = [v for v in vals if v not in ("", "None")]
    return sorted(vals)

col1, col2, col3 = st.columns(3)
with col1:
    sel_main = st.multiselect("メインカテゴリ", options("メインカテゴリ"))
with col2:
    sel_sub = st.multiselect("サブカテゴリ", options("サブカテゴリ"))
with col3:
    sel_goal = st.multiselect("広告目的", options("広告目的"))

col4, col5, col6 = st.columns(3)
with col4:
    sel_area = st.multiselect("地方", options("地方"))
with col5:
    sel_pref = st.multiselect("都道府県", options("都道府県"))
with col6:
    sel_seg = st.multiselect("棟数セグメント", options("building_count"))

# 👇 フィルター条件サマリ表示用の共通関数
def show_filter_summary():
    filter_items = [
        ("📁 メインカテゴリ", sel_main),
        ("🗂️ サブカテゴリ", sel_sub),
        ("🏠 棟数セグメント", sel_seg),
        ("🌏 地方", sel_area),
        ("🗾 都道府県", sel_pref),
        ("🎯 広告目的", sel_goal),
    ]
    filter_text = "｜".join([
        f"{label}：{'すべて' if not vals else ' / '.join(map(str, vals))}"
        for label, vals in filter_items
    ])
    st.markdown(
        f"<span style='font-size:12px; color:#666;'>{filter_text}</span>",
        unsafe_allow_html=True,
    )

# 共通フィルター関数（キャンペーン単位・明細どちらにも使う）
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    cond = pd.Series(True, index=df.index)
    if sel_main and "メインカテゴリ" in df.columns:
        cond &= df["メインカテゴリ"].isin(sel_main)
    if sel_sub and "サブカテゴリ" in df.columns:
        cond &= df["サブカテゴリ"].isin(sel_sub)
    if sel_goal and "広告目的" in df.columns:
        cond &= df["広告目的"].isin(sel_goal)
    if sel_area and "地方" in df.columns:
        cond &= df["地方"].isin(sel_area)
    if sel_pref and "都道府県" in df.columns:
        cond &= df["都道府県"].isin(sel_pref)
    if sel_seg and "building_count" in df.columns:
        cond &= df["building_count"].isin(sel_seg)
    return df.loc[cond].copy()


df_campaign_f = apply_filters(df_campaign)
df_raw_f = apply_filters(df_raw)

if df_campaign_f.empty:
    st.warning("該当データがありません。条件を変えて再度お試しください。")
    st.stop()

# ──────────────────────────────────────────────
# ① 達成率一覧（キャンペーン単位）※Ad Drive 集計に準拠
# ──────────────────────────────────────────────
st.markdown("### 💠 達成率一覧（キャンペーン単位）")

display_cols = [
    "CampaignId",
    "キャンペーン名",
    "client_name",
    "building_count",
    "配信月",
    "広告媒体",
    "メインカテゴリ",
    "サブカテゴリ",
    "広告目的",
    "地方",
    "都道府県",
    "Cost",
    "conv_total",
    "Impressions",
    "Clicks",
    "CPA",
    "CVR",
    "CTR",
    "CPC",
    "CPM",
    "CPA_best", "CPA_good", "CPA_min", "CPA_評価",
    "CVR_best", "CVR_good", "CVR_min", "CVR_評価",
    "CTR_best", "CTR_good", "CTR_min", "CTR_評価",
    "CPC_best", "CPC_good", "CPC_min", "CPC_評価",
    "CPM_best", "CPM_good", "CPM_min", "CPM_評価",
    "目標CPA",
]

disp = df_campaign_f[[c for c in display_cols if c in df_campaign_f.columns]].copy()

# 表示フォーマット（金額・％・件数）
for c in ["Cost", "CPA", "CPC", "CPM", "目標CPA"]:
    if c in disp.columns:
        disp[c] = disp[c].apply(lambda v: f"¥{v:,.0f}" if pd.notna(v) else "-")
for c in ["CVR", "CTR"]:
    if c in disp.columns:
        disp[c] = disp[c].apply(lambda v: f"{v*100:.2f}%" if pd.notna(v) else "-")
for c in ["Impressions", "Clicks", "conv_total"]:
    if c in disp.columns:
        disp[c] = disp[c].apply(lambda v: f"{int(v):,}" if pd.notna(v) else "-")

st.dataframe(disp, use_container_width=True, hide_index=True)

# ──────────────────────────────────────────────
# ② 月別推移グラフ（実績 vs KPI）※Ad Drive と同じロジック
# ──────────────────────────────────────────────
st.markdown("### 📈 月別推移グラフ（実績 vs KPI）")

def get_label(val, indicator, is_kpi=False):
    if pd.isna(val):
        return ""
    if indicator in ["CPA", "CPC", "CPM"]:
        return f"¥{val:,.0f}"
    elif indicator in ["CVR", "CTR"]:
        if is_kpi:
            return f"{val:.1f}%"
        else:
            return f"{val*100:.1f}%"
    else:
        return f"{val}"


# KPI はひとまず「注文住宅･規格住宅 × 完成見学会 × コンバージョン」で固定（従来どおり）
kpi_row = df_kpi[
    (df_kpi["メインカテゴリ"] == "注文住宅･規格住宅")
    & (df_kpi["サブカテゴリ"] == "完成見学会")
    & (df_kpi["広告目的"] == "コンバージョン")
].iloc[0]

kpi_dict = {
    "CPA": kpi_row["CPA_good"],
    "CVR": kpi_row["CVR_good"],
    "CTR": kpi_row["CTR_good"],
    "CPC": kpi_row["CPC_good"],
    "CPM": kpi_row["CPM_good"],
}

if "配信月_dt" in df_raw_f.columns and not df_raw_f.empty:
    df_month = df_raw_f.copy()

    monthly = (
        df_month.groupby("配信月_dt", as_index=False)
        .agg(
            Cost=("Cost", "sum"),
            conv_total=("conv_total", "sum"),
            Impressions=("Impressions", "sum"),
            Clicks=("Clicks", "sum"),
        )
    )

    # Ad Drive と同じ計算式で再計算
    monthly["CPA"] = monthly.apply(
        lambda r: r["Cost"] / r["conv_total"] if r["conv_total"] > 0 else np.nan,
        axis=1,
    )
    monthly["CVR"] = monthly.apply(
        lambda r: r["conv_total"] / r["Clicks"] if r["Clicks"] > 0 else np.nan,
        axis=1,
    )
    monthly["CTR"] = monthly.apply(
        lambda r: r["Clicks"] / r["Impressions"] if r["Impressions"] > 0 else np.nan,
        axis=1,
    )
    monthly["CPC"] = monthly.apply(
        lambda r: r["Cost"] / r["Clicks"] if r["Clicks"] > 0 else np.nan,
        axis=1,
    )
    monthly["CPM"] = monthly.apply(
        lambda r: (r["Cost"] * 1000 / r["Impressions"]) if r["Impressions"] > 0 else np.nan,
        axis=1,
    )

    indicators = ["CPA", "CVR", "CTR", "CPC", "CPM"]
    for ind in indicators:
        st.markdown(f"#### 📉 {ind} 推移")
        # 👉 各推移グラフごとにフィルターサマリを表示
        show_filter_summary()

        df_plot = monthly[["配信月_dt", ind]].dropna().sort_values("配信月_dt").copy()
        if df_plot.empty:
            st.info("この条件ではグラフ用のデータがありません。")
            continue

        # KPI（CVR・CTR は % → 小数へ）
        kpi_val = kpi_dict[ind]
        if ind in ["CVR", "CTR"]:
            kpi_val = kpi_val / 100.0

        df_plot["実績値"] = df_plot[ind]
        df_plot["実績値_label"] = df_plot["実績値"].apply(
            lambda v: f"{v*100:.1f}%" if ind in ["CVR", "CTR"] else get_label(v, ind)
        )
        kpi_label = (
            f"{kpi_val*100:.1f}%"
            if ind in ["CVR", "CTR"]
            else get_label(kpi_val, ind, is_kpi=True)
        )

        df_plot["目標値"] = kpi_val
        df_plot["目標値_label"] = kpi_label

        # 昨年同月（便宜上、同じ系列を 1 年シフト）
        df_lastyear = df_plot.copy()
        df_lastyear["配信月_dt"] = df_lastyear["配信月_dt"] + pd.DateOffset(years=1)

        # 今月までに制限
        today = pd.Timestamp.today().normalize()
        current_month_start = pd.Timestamp(today.year, today.month, 1)
        df_plot = df_plot[df_plot["配信月_dt"] <= current_month_start]
        df_lastyear = df_lastyear[df_lastyear["配信月_dt"] <= current_month_start]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df_plot["配信月_dt"],
                y=df_plot["実績値"],
                mode="lines+markers+text",
                name="実績値",
                text=df_plot["実績値_label"],
                textposition="top center",
                hovertemplate="%{x|%Y/%m}<br>実績値：%{text}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_lastyear["配信月_dt"],
                y=df_lastyear["実績値"],
                mode="lines+markers",
                name="昨年同月",
                opacity=0.3,
                hovertemplate="%{x|%Y/%m}<br>昨年同月：%{y}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df_plot["配信月_dt"],
                y=df_plot["目標値"],
                mode="lines+markers+text",
                name="目標値",
                text=[kpi_label] * len(df_plot),
                textposition="top center",
                line=dict(dash="dash"),
                hovertemplate="%{x|%Y/%m}<br>目標値：%{text}<extra></extra>",
            )
        )

        if ind in ["CVR", "CTR"]:
            fig.update_layout(
                yaxis_title=f"{ind} (%)",
                xaxis_title="配信月",
                xaxis_tickformat="%Y/%m",
                yaxis_tickformat=".1%",
                height=380,
                hovermode="x unified",
            )
        else:
            fig.update_layout(
                yaxis_title=ind,
                xaxis_title="配信月",
                xaxis_tickformat="%Y/%m",
                height=380,
                hovermode="x unified",
            )

        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("配信月情報がないため、月別推移グラフは表示できません。")

# ──────────────────────────────────────────────
# ③ 都道府県別 パフォーマンス（CPA）※Ad Drive ロジック準拠
# ──────────────────────────────────────────────
st.markdown("### 🗾 都道府県別 CPA")
# ここでもフィルター条件を表示
show_filter_summary()

df_pref = df_campaign_f.copy()
if not df_pref.empty and "都道府県" in df_pref.columns:
    pref_agg = (
        df_pref.groupby("都道府県", as_index=False)
        .agg(
            Cost=("Cost", "sum"),
            conv_total=("conv_total", "sum"),
        )
    )
    pref_agg["CPA"] = np.where(
        pref_agg["conv_total"] > 0,
        pref_agg["Cost"] / pref_agg["conv_total"],
        np.nan,
    )
    pref_agg = pref_agg.dropna(subset=["CPA"]).sort_values("CPA")

    fig_pref = px.bar(
        pref_agg,
        x="都道府県",
        y="CPA",
        labels={"CPA": "CPA", "都道府県": "都道府県"},
    )
    fig_pref.update_layout(height=420)

    st.plotly_chart(fig_pref, use_container_width=True)
else:
    st.info("都道府県別集計に利用できるデータがありません。")
