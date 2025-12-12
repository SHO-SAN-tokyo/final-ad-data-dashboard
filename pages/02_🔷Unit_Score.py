import streamlit as st 
from google.cloud import bigquery
import pandas as pd
import numpy as np
import requests

# ──────────────────────
# ログイン認証
# ──────────────────────
from auth import require_login
require_login()

# ──────────────────────
# コンテンツ
# ──────────────────────
st.set_page_config(page_title="Unit Drive", layout="wide")

# グローバルなボタンのスタイル（他ページでも使う想定で残す）
st.markdown("""
<style>
div.stButton > button {
    font-size: 9px !important;
    line-height: 1.1 !important;
    padding: 2px 8px !important;
    height: auto !important;
}
button[kind] {
    font-size: 9px !important;
}
</style>
""", unsafe_allow_html=True)

# --- タイトルのみ表示 ---
st.markdown(
    "<h1 style='display:inline-block;margin-bottom:0;'>🔷 Unit Score ／ユニット・個人成績</h1>",
    unsafe_allow_html=True
)

# st.subheader（”📊 広告TM パフォーマンス”）

info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

@st.cache_data(show_spinner="データ取得中…")
def load_data():
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Unit_Drive_Ready_View").to_dataframe()
    return df

df = load_data()

# 📅 配信月フィルタ（新しい月順、Noneは最下部・現在月をデフォルト選択）
raw_months = df["配信月"].unique().tolist()

def _parse_month(v):
    if pd.isna(v):
        return None
    s = str(v)
    for fmt in ("%Y-%m", "%Y/%m", "%Y%m", "%Y.%m"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s, errors="raise")
    except Exception:
        return None

# ★ ここをif/elseの通常形に変更（Falseが表示される問題の修正ポイント）
valid, invalid = [], []
for m in raw_months:
    pm = _parse_month(m)
    if pm is not None:
        valid.append(m)
    else:
        invalid.append(m)

# 並び：新しい月 → それ以外 → None を最下部
valid_sorted = [m for _, m in sorted(((_parse_month(m), m) for m in valid), key=lambda t: t[0], reverse=True)]
invalid_no_none = [m for m in invalid if m is not None]
invalid_sorted = sorted(invalid_no_none, key=lambda x: str(x))
has_none = any(pd.isna(x) or x is None for x in raw_months)
month_options = valid_sorted + invalid_sorted + ([None] if has_none else [])

# 現在月をデフォルト選択（あれば）
now_tokyo = pd.Timestamp.now(tz="Asia/Tokyo")
candidates = [
    now_tokyo.strftime("%Y-%m"),
    now_tokyo.strftime("%Y/%m"),
    now_tokyo.strftime("%Y%m"),
    now_tokyo.strftime("%Y.%m"),
]
default_month = next((c for c in candidates if c in month_options), None)
default_sel = [default_month] if default_month else []

sel_month = st.multiselect("📅 配信月", month_options, default=default_sel, placeholder="すべて")
if sel_month:
    df = df[df["配信月"].isin(sel_month)]


# ▼ ここからキャンペーン単位で合算（配信月+CampaignId+クライアント名でgroupby）
group_cols = ["配信月", "CampaignId", "クライアント名"]

# 代表行がブレないよう一応並べ替え（存在するキーのみ）
sort_keys = [k for k in ["配信月","CampaignId","クライアント名","配信終了日","配信開始日","日付"] if k in df.columns]
if sort_keys:
    df = df.sort_values(sort_keys)

# 閾値列も保持（後段で再評価に使う）
agg_dict = {
    "キャンペーン名": "last",
    "campaign_uuid": "last",
    "担当者": "last",
    "所属": "last",
    "フロント": "last",
    "雇用形態": "last",
    "予算": "sum",
    "フィー": "sum",
    "消化金額": "sum",
    "コンバージョン数": "sum",
    "クリック数": "sum" if "クリック数" in df.columns else "last",
    "CVR": "last",
    "CTR": "last",
    "CPC": "last",
    "CPM": "last",
    "canvaURL": "last",
    "メインカテゴリ": "last",
    "サブカテゴリ": "last",
    "広告媒体": "last",
    "広告目的": "last",
    "注力度": "last",
    "配信開始日": "last",
    "配信終了日": "last",
    "CPA_best": "max",
    "CPA_good": "max",
    "CPA_min":  "max",
    "目標CPA":   "max",
    "CPA_KPI_評価": "last",
    "CPC_KPI_評価": "last",
    "CPM_KPI_評価": "last",
    "CVR_KPI_評価": "last",
    "CTR_KPI_評価": "last",
    "個別CPA_達成": "last",
    "達成状況": "last"
}
df = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

# ▼ CPA/CVRを再計算
df["CPA"] = df["消化金額"] / df["コンバージョン数"].replace(0, np.nan)
if "クリック数" in df.columns:
    df["CVR"] = df["コンバージョン数"] / df["クリック数"].replace(0, np.nan)

# ───────── 再評価（“コンバージョン”を含む） ─────────
is_conv = df["広告目的"].fillna("").str.contains("コンバージョン", na=False)
has_cpa = df["CPA"].notna()

# 評価列は最初から “string” dtype で初期化
df["CPA_KPI_評価"] = pd.Series(pd.NA, index=df.index, dtype="string")

# 評価外（コンバージョン以外）
df.loc[~is_conv, "CPA_KPI_評価"] = "評価外"

# 閾値が存在するか
has_best = df["CPA_best"].notna()

# 各評価用の条件
cond_best = is_conv & has_cpa & has_best & (df["CPA"] <= df["CPA_best"])
cond_good = is_conv & has_cpa & df["CPA_good"].notna() & (df["CPA"] <= df["CPA_good"])
cond_min  = is_conv & has_cpa & df["CPA_min"].notna()  & (df["CPA"] <= df["CPA_min"])

# 順に上書き
df.loc[cond_best, "CPA_KPI_評価"] = "◎"
df.loc[~df["CPA_KPI_評価"].isin(["◎"]) & cond_good, "CPA_KPI_評価"] = "〇"
df.loc[~df["CPA_KPI_評価"].isin(["◎","〇"]) & cond_min, "CPA_KPI_評価"] = "△"

# 未設定かつ（CV目的 かつ CPAとbestが有効）→ ✕
df.loc[
    df["CPA_KPI_評価"].isna() & is_conv & has_cpa & has_best,
    "CPA_KPI_評価"
] = "✕"

# ===== 個別CPA_達成（安全に判定） =====
df["個別CPA_達成"] = pd.Series(pd.NA, index=df.index, dtype="string")

mask_target = df["目標CPA"].notna()
mask_cpa    = df["CPA"].notna()
mask_valid  = mask_target & mask_cpa

df.loc[~mask_target, "個別CPA_達成"] = "個別目標なし"
df.loc[mask_valid & (df["CPA"] <= df["目標CPA"]), "個別CPA_達成"] = "〇"
df.loc[mask_valid & (df["CPA"] >  df["目標CPA"]), "個別CPA_達成"] = "✕"

# ===== 達成状況（安全に判定） =====
# ルール：
# - 広告目的が「コンバージョン」を含まない -> 「評価外」
# - それ以外は、(CPA<=CPA_good) または (CPA<=目標CPA) のどちらか満たせば「達成」、そうでなければ「未達成」
df["達成状況"] = pd.Series(pd.NA, index=df.index, dtype="string")

mask_conv   = df["広告目的"].fillna("").str.contains("コンバージョン", case=False, na=False)
mask_cpa    = df["CPA"].notna()
mask_cpa_go = df["CPA_good"].notna()
mask_target = df["目標CPA"].notna()

# デフォルト：評価対象外
df.loc[~mask_conv, "達成状況"] = "評価外"

# コンバージョン目的のみ判定
mask_judge = mask_conv & mask_cpa

# まず未達成で埋める
df.loc[mask_judge, "達成状況"] = "未達成"

# 達成条件： (CPA <= CPA_good) or (CPA <= 目標CPA)
df.loc[mask_judge & mask_cpa_go & (df["CPA"] <= df["CPA_good"]), "達成状況"] = "達成"
df.loc[mask_judge & mask_target & (df["CPA"] <= df["目標CPA"]),  "達成状況"] = "達成"

# ===== ここから表示用の補助関数 =====
def safe_cpa(cost, cv):
    return cost / cv if cv > 0 else np.nan

def fill_cpa_eval_for_display(df_in: pd.DataFrame) -> pd.DataFrame:
    """表示専用：CV=0 かつ CPA=0円 かつ コンバージョン目的 かつ 評価が空/NaN → '✕' に置換"""
    d = df_in.copy()
    if "CPA_KPI_評価" not in d.columns:
        return d
    is_conv = d.get("広告目的", pd.Series(index=d.index)).fillna("").str.contains("コンバージョン", na=False)
    zero_cv  = d.get("コンバージョン数", pd.Series(index=d.index)).fillna(0).astype(float).eq(0)
    zero_cpa = d.get("CPA", pd.Series(index=d.index)).fillna(0).astype(float).eq(0)
    blank_eval = d["CPA_KPI_評価"].isna() | (d["CPA_KPI_評価"].astype(str).str.strip() == "")
    d.loc[is_conv & zero_cv & zero_cpa & blank_eval, "CPA_KPI_評価"] = "✕"
    return d

# フィルター項目
latest = df.copy()
numeric_cols = latest.select_dtypes(include=["number"]).columns
latest[numeric_cols] = latest[numeric_cols].replace([np.inf, -np.inf], 0).fillna(0)
latest = latest[latest["所属"].notna()]
latest = latest[latest["所属"].apply(lambda x: isinstance(x, str))]

unit_options = sorted(latest["所属"].dropna().unique())
person_options = sorted(latest["担当者"].dropna().astype(str).unique())
front_options = sorted(latest["フロント"].dropna().astype(str).unique())
employment_options = sorted(latest["雇用形態"].dropna().astype(str).unique())
focus_options = sorted(latest["注力度"].dropna().astype(str).unique())
maincat_options = sorted(latest["メインカテゴリ"].dropna().astype(str).unique())
subcat_options = sorted(latest["サブカテゴリ"].dropna().astype(str).unique())

# ★ 初期フィルター
default_employment = ["インターン"] if "インターン" in employment_options else []
default_maincat = [x for x in maincat_options if x not in ["分譲マンション"]]
default_subcat = [x for x in subcat_options if x not in ["認知", "採用", "ページ流入"]]

# UI（上段：注力度＋メインカテゴリ、下段：サブカテゴリ）
f1, f2, f3, f4 = st.columns(4)
with f1:
    unit_filter = st.multiselect("🏷️ Unit", unit_options, placeholder="すべて")
with f2:
    person_filter = st.multiselect("👤 担当者", person_options, placeholder="すべて")
with f3:
    front_filter = st.multiselect("👤 フロント", front_options, placeholder="すべて")
with f4:
    employment_filter = st.multiselect("🏢 雇用形態", employment_options, default=default_employment, key="employment_type")

row1_c1, row1_c2 = st.columns(2)
with row1_c1:
    focus_filter = st.multiselect("📌 注力度", focus_options, placeholder="すべて")
with row1_c2:
    maincat_filter = st.multiselect("📁 メインカテゴリ", maincat_options, default=default_maincat, key="maincat")

row2_full, = st.columns(1)
with row2_full:
    subcat_filter = st.multiselect("📂 サブカテゴリ", subcat_options, default=default_subcat, key="subcat")

# --- 状況表示
st.markdown(f"""
<div style='font-size: 0.9rem; line-height: 1.8;'>
📅 配信月: <b>{sel_month or 'すべて'}</b><br>
🏷️Unit: <b>{unit_filter or 'すべて'}</b><br>
👤担当者: <b>{person_filter or 'すべて'}</b><br>
👤フロント: <b>{front_filter or 'すべて'}</b><br>
🏢雇用形態: <b>{employment_filter or 'すべて'}</b><br>
📌注力度: <b>{focus_filter or 'すべて'}</b><br>
📁メインカテゴリ: <b>{maincat_filter or 'すべて'}</b><br>
📂サブカテゴリ: <b>{subcat_filter or 'すべて'}</b>
</div>
""", unsafe_allow_html=True)

# --- フィルター適用
df_filtered = latest.copy()
if unit_filter:
    df_filtered = df_filtered[df_filtered["所属"].isin(unit_filter)]
if person_filter:
    df_filtered = df_filtered[df_filtered["担当者"].isin(person_filter)]
if front_filter:
    df_filtered = df_filtered[df_filtered["フロント"].isin(front_filter)]
if employment_filter:
    df_filtered = df_filtered[df_filtered["雇用形態"].isin(employment_filter)]
if focus_filter:
    df_filtered = df_filtered[df_filtered["注力度"].isin(focus_filter)]
if maincat_filter:
    df_filtered = df_filtered[df_filtered["メインカテゴリ"].isin(maincat_filter)]
if subcat_filter:
    df_filtered = df_filtered[df_filtered["サブカテゴリ"].isin(subcat_filter)]

# ★ フィルター後 0件なら停止（余白＋メッセージ）
if df_filtered.empty:
    st.markdown("<div style='height: 1.0rem;'></div>", unsafe_allow_html=True)
    st.info("該当データがありません。フィルター条件を見直してください。")
    st.stop()

# -----------------------------
# 1. Unitごとのサマリー（2軸）
# -----------------------------
def campaign_key(df_):
    return df_["配信月"].astype(str) + "_" + df_["CampaignId"].astype(str) + "_" + df_["クライアント名"].astype(str)

unit_group = df_filtered.groupby("所属", dropna=False)
unit_summary = []
for unit, group in unit_group:
    group_conv = group[group["広告目的"].fillna("").str.contains("コンバージョン", na=False)]
    camp_count_conv = campaign_key(group_conv).nunique()
    camp_count_all = campaign_key(group).nunique()
    spend_conv = group_conv["消化金額"].sum()
    spend_all = group["消化金額"].sum()
    total_cv = group_conv["コンバージョン数"].sum()
    cpa = safe_cpa(spend_conv, total_cv)
    unit_summary.append({
        "所属": unit,
        "CPA": cpa,
        "キャンペーン数(コンバージョン)": camp_count_conv,
        "キャンペーン数(すべて)": camp_count_all,
        "消化金額(コンバージョン)": spend_conv,
        "消化金額(すべて)": spend_all,
        "CV": total_cv,
    })

unit_summary_df = pd.DataFrame(unit_summary)

if unit_summary_df.empty:
    st.info("（Unit集計）該当データがありません。")
else:
    unit_summary_df = unit_summary_df.sort_values("所属")

    # --- Unit別色マップ
    unit_colors = ["#c0e4eb", "#cbebb5", "#ffdda6"]
    unit_color_map = {unit: unit_colors[i % len(unit_colors)] for i, unit in enumerate(unit_summary_df["所属"].unique())}

    # --- Unitカード ---
    st.write("#### 🍋🍋‍🟩 Unitごとのスコア 🍒🍏")

    # 🆕 全体CPA
    overall_conv = df_filtered[df_filtered["広告目的"].fillna("").str.contains("コンバージョン", na=False)]
    overall_camp_count_conv = campaign_key(overall_conv).nunique()
    overall_camp_count_all = campaign_key(df_filtered).nunique()
    overall_spend_conv = overall_conv["消化金額"].sum()
    overall_spend_all = df_filtered["消化金額"].sum()
    overall_cv = overall_conv["コンバージョン数"].sum()
    overall_cpa = safe_cpa(overall_spend_conv, overall_cv)

    # NaN対策（全部CV=0のときなど）
    if pd.isna(overall_cpa) or not np.isfinite(overall_cpa):
        overall_cpa_value = 0.0
    else:
        overall_cpa_value = overall_cpa

    avg_cols = st.columns(3)
    with avg_cols[0]:
        st.markdown(f"""
        <div style='background-color: #edf2ff; padding: 1.2rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem; border: 1px solid #d0d7ff;'>
            <div style='font-size: 1.4rem; font-weight: bold; text-align: center;'>全体CPA</div>
            <div style='font-size: 1.3rem; font-weight: bold;'>¥{overall_cpa_value:,.0f}</div>
            <div style='font-size: 0.8rem; margin-top: 0.7rem; text-align:center;'>
                キャンペーン数(CV目的)  :  {int(overall_camp_count_conv)}<br>
                キャンペーン数(すべて)  :  {int(overall_camp_count_all)}<br>
                消化金額(CV目的)  :  ¥{int(overall_spend_conv):,}<br>
                消化金額(すべて)  :  ¥{int(overall_spend_all):,}<br>
                CV数  :  {int(overall_cv)}
            </div>
        </div>
        """, unsafe_allow_html=True)

    # 既存：Unitごとのカード
    unit_cols = st.columns(3)
    for idx, row in unit_summary_df.iterrows():
        with unit_cols[idx % 3]:
            st.markdown(f"""
            <div style='background-color: {unit_color_map.get(row["所属"], "#f0f0f0")}; padding: 1.2rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
                <div style='font-size: 1.6rem; font-weight: bold; text-align: center;'>{row['所属']}</div>
                <div style='font-size: 1.3rem; font-weight: bold;'>¥{row['CPA']:,.0f}</div>
                <div style='font-size: 0.8rem; margin-top: 0.7rem; text-align:center;'>
                    キャンペーン数(CV目的)  :  {int(row["キャンペーン数(コンバージョン)"])}<br>
                    キャンペーン数(すべて)  :  {int(row["キャンペーン数(すべて)"])}<br>
                    消化金額(CV目的)  :  ¥{int(row["消化金額(コンバージョン)"]):,}<br>
                    消化金額(すべて)  :  ¥{int(row["消化金額(すべて)"]):,}<br>
                    CV数  :  {int(row["CV"])}
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top: 1.3rem;'></div>", unsafe_allow_html=True)


# -----------------------------
# 2. 担当者ごとのスコア（2軸）
# -----------------------------
person_group = df_filtered.groupby("担当者", dropna=False)

person_summary = []
for person, group in person_group:
    group_conv = group[group["広告目的"].fillna("").str.contains("コンバージョン", na=False)]
    camp_count_conv = group_conv.shape[0]
    spend_conv = group_conv["消化金額"].sum()
    camp_count_all = group.shape[0]
    spend_all = group["消化金額"].sum()
    total_cv = group_conv["コンバージョン数"].sum()
    cpa = safe_cpa(spend_conv, total_cv)
    person_summary.append({
        "担当者": person,
        "CPA": cpa,
        "キャンペーン数(コンバージョン)": camp_count_conv,
        "キャンペーン数(すべて)": camp_count_all,
        "消化金額(コンバージョン)": spend_conv,
        "消化金額(すべて)": spend_all,
        "CV": total_cv,
    })
person_summary_df = pd.DataFrame(person_summary)

if person_summary_df.empty:
    st.info("（担当者集計）該当データがありません。")
else:
    person_summary_df = person_summary_df.sort_values("担当者")
    person_summary_df = person_summary_df.merge(
        latest[["担当者", "所属"]].drop_duplicates(), on="担当者", how="left"
    )

    # Unit色マップ（Unitカードが描画されなかった場合に備え簡易生成）
    if "所属" in person_summary_df.columns and not person_summary_df["所属"].dropna().empty:
        units_for_color = person_summary_df["所属"].fillna("NA").unique().tolist()
    else:
        units_for_color = ["NA"]
    unit_colors = ["#c0e4eb", "#cbebb5", "#ffdda6"]
    unit_color_map = {u: unit_colors[i % len(unit_colors)] for i, u in enumerate(units_for_color)}

    person_cols = st.columns(4)
    for idx, row in person_summary_df.iterrows():
        color = unit_color_map.get(row.get("所属"), "#f0f0f0")
        with person_cols[idx % 4]:
            st.markdown(f"""
            <div style='background-color: {color}; padding: 1.2rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
                <h4 style='font-size: 1.2rem; padding: 10px 0 10px 16px;'>{row['担当者']}</h4>
                <div style='font-size: 1.2rem; font-weight: bold;'>¥{row['CPA']:,.0f}</div>
                <div style='font-size: 0.8rem; margin-top: 0.5rem; text-align:center;'>
                    キャンペーン数(CV目的)  :  {int(row["キャンペーン数(コンバージョン)"])}<br>
                    キャンペーン数(すべて)  :  {int(row["キャンペーン数(すべて)"])}<br>
                    消化金額(CV目的)  :  ¥{int(row["消化金額(コンバージョン)"]):,}<br>
                    消化金額(すべて)  :  ¥{int(row["消化金額(すべて)"]):,}<br>
                    CV数  :  {int(row["CV"])}
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top: 1.3rem;'></div>", unsafe_allow_html=True)

# -----------------------------
# 3. Unitごとの達成率（コンバージョン目的のみ）
# -----------------------------
st.write("#### 🏢 Unitごとの達成率（コンバージョン目的のみ）")
if "達成状況" in df_filtered.columns:
    conv_df = df_filtered[df_filtered["広告目的"].fillna("").str.contains("コンバージョン", na=False)].copy()
    conv_df["キャンペーンキー"] = (
        conv_df["配信月"].astype(str) + "_" +
        conv_df["CampaignId"].astype(str) + "_" +
        conv_df["クライアント名"].astype(str)
    )
    df_uniq = conv_df.drop_duplicates("キャンペーンキー")
    unit_agg = (
        df_uniq.groupby("所属", dropna=False)
        .agg(
            campaign_count=("キャンペーンキー", "nunique"),
            達成件数=("達成状況", lambda x: (x == "達成").sum())
        )
        .reset_index()
    )

    if unit_agg.empty:
        st.info("（Unit達成率）該当データがありません。")
    else:
        unit_agg["達成率"] = unit_agg["達成件数"] / unit_agg["campaign_count"]
        unit_agg = unit_agg.sort_values("達成率", ascending=False)

        # 🆕 全体達成率
        total_campaigns = int(unit_agg["campaign_count"].sum())
        total_achieved = int(unit_agg["達成件数"].sum())
        overall_rate = (total_achieved / total_campaigns) if total_campaigns > 0 else np.nan

        avg_cols = st.columns(3)
        with avg_cols[0]:
            rate_disp = f"{overall_rate:.0%}" if total_campaigns > 0 else "-%"
            st.markdown(f"""
            <div style='background-color: #e6f4ea; padding: 1rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem; border: 1px solid #c6e6cf;'>
                <h5 style='font-size: 1.2rem; padding: 10px 0px 10px 15px; font-weight:bold;'>全体達成率</h5>
                <div style='font-size: 1.2rem; font-weight: bold; padding-bottom: 5px;'>{rate_disp}</div>
                <div style='font-size: 0.8rem; padding-bottom: 5px;'>
                    キャンペーン数(CV目的)  :  {total_campaigns}<br>
                    達成数: {total_achieved}
                </div>
            </div>
            """, unsafe_allow_html=True)

        # 既存：Unitごとのカード
        unit_cols = st.columns(3)
        for idx, row in unit_agg.iterrows():
            with unit_cols[idx % 3]:
                st.markdown(f"""
                <div style='background-color: #f0f5eb; padding: 1rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
                    <h5 style='font-size: 1.2rem; padding: 10px 0px 10px 15px; font-weight:bold;'>{row["所属"]}</h5>
                    <div style='font-size: 1.2rem; font-weight: bold; padding-bottom: 5px;'>{row["達成率"]:.0%}</div>
                    <div style='font-size: 0.8rem; padding-bottom: 5px;'>
                        キャンペーン数(CV目的)  :  {int(row["campaign_count"])}<br>
                        達成数: {int(row["達成件数"])}
                    </div>
                </div>
                """, unsafe_allow_html=True)


st.markdown("<div style='margin-top: 1.3rem;'></div>", unsafe_allow_html=True)

# -----------------------------
# 3. 担当者ごとの達成率（コンバージョン目的のみ）
# -----------------------------
st.write("#### 👨‍💼 担当者ごとの達成率（コンバージョン目的のみ）")
if "達成状況" in df_filtered.columns:
    conv_df = df_filtered[df_filtered["広告目的"].fillna("").str.contains("コンバージョン", na=False)]
    person_agg = conv_df.groupby("担当者", dropna=False).agg(
        campaign_count=("キャンペーン名", "count"),
        達成件数=("達成状況", lambda x: (x == "達成").sum())
    ).reset_index()

    if person_agg.empty:
        st.info("（担当者達成率）該当データがありません。")
    else:
        person_agg["達成率"] = person_agg["達成件数"] / person_agg["campaign_count"]
        person_agg = person_agg.sort_values("達成率", ascending=False)
        person_cols = st.columns(5)
        for idx, row in person_agg.iterrows():
            with person_cols[idx % 5]:
                st.markdown(f"""
                <div style='background-color: #f0f5eb; padding: 1rem; border-radius: 1rem; text-align: center; margin-bottom: 1.2rem;'>
                    <h5 style='font-size: 1.2rem; padding: 10px 0px 10px 15px;'>{row["担当者"]}</h5>
                    <div style='font-size: 1.2rem; font-weight: bold; padding-bottom: 5px;'>{row["達成率"]:.0%}</div>
                    <div style='font-size: 0.8rem; padding-bottom: 5px;'>
                        キャンペーン数(CV目的)  :  {int(row["campaign_count"])}<br>
                        達成数: {int(row["達成件数"])}
                    </div>
                </div>
                """, unsafe_allow_html=True)

st.markdown("<div style='margin-top: 2rem;'></div>", unsafe_allow_html=True)

# ▼ キャンペーン一覧
st.write("#### 📋 配信キャンペーン一覧（最大1,000件）")
columns_to_show = [
    "campaign_uuid","配信月","キャンペーン名","担当者","所属","フロント","雇用形態",
    "予算","フィー","クライアント名","消化金額","canvaURL",
    "カテゴリ","媒体","広告目的",
    "コンバージョン数","CPA","CVR","CTR","CPC","CPM",
    "CPA_KPI_評価","個別CPA_達成","CTR_KPI_評価","CPC_KPI_評価","CPM_KPI_評価"
]
columns_to_show = [col for col in columns_to_show if col in df_filtered.columns]

# ▼ 列名だけ一時的にリネーム
rename_dict = {"campaign_uuid": "キャンペーン固有ID"}
display_df = df_filtered[columns_to_show].rename(columns=rename_dict)

# ① 表示専用の評価補正（CV=0 & CPA=0 & コンバージョン目的 & 評価空→'✕'）
display_df_disp = fill_cpa_eval_for_display(display_df)

# ▼ キャンペーン固有ID順に並び替え（昇順）
if "キャンペーン固有ID" in display_df_disp.columns and not display_df_disp.empty:
    display_df_disp = display_df_disp.sort_values("キャンペーン固有ID")  # 昇順

styled_table = display_df_disp.head(1000).style.format({
    "予算": "¥{:,.0f}",
    "フィー": "¥{:,.0f}",
    "消化金額": "¥{:,.0f}",
    "コンバージョン数": "{:,.0f}",
    "CPA": "¥{:,.0f}",
    "CVR": "{:.1%}",
    "CTR": "{:.1%}",
    "CPC": "¥{:,.0f}",
    "CPM": "¥{:,.0f}"
})
st.dataframe(styled_table, use_container_width=True)

st.markdown("<div style='margin-top: 2rem;'></div>", unsafe_allow_html=True)

# --- 達成キャンペーン一覧 ---
if "達成状況" in df_filtered.columns:
    st.write("#### 👍 達成キャンペーン一覧")
    achieved = df_filtered[(df_filtered["達成状況"] == "達成") & (df_filtered["広告目的"].fillna("").str.contains("コンバージョン", na=False))]
    if not achieved.empty:
        cols = [
            "配信月", "キャンペーン名", "担当者", "所属",
            "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"
        ]
        display_cols = [c for c in cols if c in achieved.columns]

        achieved_disp = fill_cpa_eval_for_display(achieved[display_cols])

        st.dataframe(
            achieved_disp.style.format({
                "CPA": "¥{:,.0f}",
                "目標CPA": "¥{:,.0f}"
            }),
            use_container_width=True
        )
    else:
        st.info("達成キャンペーンがありません。")

    st.markdown("<div style='margin-top: 2rem;'></div>", unsafe_allow_html=True)

    # --- 未達成キャンペーン一覧 ---
    st.write("#### 💤 未達成キャンペーン一覧")

    # 1) 抽出にも“表示用補正”を適用してから使う（CV=0 & CPA=0 & 評価空 → '✕' に補正）
    df_for_missed = fill_cpa_eval_for_display(df_filtered.copy())

    # 2) コンバージョン目的 かつ CPA_KPI_評価が「✕」または空白を未達成とする
    conv_mask = df_for_missed["広告目的"].fillna("").str.contains("コンバージョン", na=False)
    eval_col  = df_for_missed["CPA_KPI_評価"].astype("string")
    is_x      = eval_col == "✕"
    is_delta  = eval_col == "△"
    is_blank  = eval_col.isna() | (eval_col.str.strip() == "")

    missed = df_for_missed[conv_mask & (is_x | is_delta | is_blank)].copy()

    if not missed.empty:
        cols = ["配信月", "キャンペーン名", "担当者", "所属",
                "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"]
        display_cols = [c for c in cols if c in missed.columns]

        # 表示整形（ここでは再補正不要。すでに fill_cpa_eval_for_display 済み）
        st.dataframe(
            missed[display_cols].style.format({
                "CPA": "¥{:,.0f}",
                "目標CPA": "¥{:,.0f}"
            }),
            use_container_width=True
        )
    else:
        st.info("未達成キャンペーンがありません。")


    st.markdown("<div style='margin-top: 1.2rem;'></div>", unsafe_allow_html=True)

    # ③ 評価外キャンペーン一覧（CPA_KPI_評価 == '評価外'）
    st.write("#### 🚫 評価外キャンペーン一覧")
    outside = df_filtered[df_filtered.get("CPA_KPI_評価", pd.Series(index=df_filtered.index)).eq("評価外")]
    if not outside.empty:
        cols = ["配信月", "キャンペーン名", "担当者", "所属", "広告目的", "CPA", "CPA_KPI_評価"]
        display_cols = [c for c in cols if c in outside.columns]
        outside_disp = outside[display_cols]
        st.dataframe(
            outside_disp.style.format({
                "CPA": "¥{:,.0f}"
            }),
            use_container_width=True
        )
    else:
        st.info("評価外のキャンペーンはありません。")
