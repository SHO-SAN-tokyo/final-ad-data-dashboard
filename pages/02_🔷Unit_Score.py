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

# 📅 配信月フィルタ
month_options = sorted(df["配信月"].dropna().unique())
sel_month = st.multiselect("📅 配信月", month_options, placeholder="すべて")
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

# 初期化：NaN（= NULL 相当）
df["CPA_KPI_評価"] = np.nan

# 評価外（コンバージョン以外）
df.loc[~is_conv, "CPA_KPI_評価"] = "評価外"

# 閾値が存在するか（best を主判定に使う：仕様に合わせる）
has_best = df["CPA_best"].notna()

# 各評価用の条件（欠損を必ず除外）
cond_best = is_conv & has_cpa & has_best & (df["CPA"] <= df["CPA_best"])
cond_good = is_conv & has_cpa & df["CPA_good"].notna() & (df["CPA"] <= df["CPA_good"])
cond_min  = is_conv & has_cpa & df["CPA_min"].notna()  & (df["CPA"] <= df["CPA_min"])

# 順に上書き
df.loc[cond_best, "CPA_KPI_評価"] = "◎"
df.loc[~df["CPA_KPI_評価"].isin(["◎"]) & cond_good, "CPA_KPI_評価"] = "〇"
df.loc[~df["CPA_KPI_評価"].isin(["◎","〇"]) & cond_min, "CPA_KPI_評価"] = "△"

# ここまでで未設定かつ（コンバージョン かつ CPAとbestが有効）→ ✕
df.loc[
    df["CPA_KPI_評価"].isna() & is_conv & has_cpa & has_best,
    "CPA_KPI_評価"
] = "✕"
# （bestが欠損 or CPA欠損）は仕様通り NaN のまま

# ===== 個別CPA_達成（安全に判定） =====
df["個別CPA_達成"] = pd.Series(pd.NA, index=df.index, dtype="string")
mask_target = df["目標CPA"].notna()
mask_cpa    = df["CPA"].notna()
mask_valid  = mask_target & mask_cpa
df.loc[~mask_target, "個別CPA_達成"] = "個別目標なし"
df.loc[mask_valid & (df["CPA"] <= df["目標CPA"]), "個別CPA_達成"] = "〇"
df.loc[mask_valid & (df["CPA"] >  df["目標CPA"]), "個別CPA_達成"] = "✕"

# ===== 達成状況（安全に判定） =====
df["達成状況"] = pd.Series(pd.NA, index=df.index, dtype="string")
mask_conv   = df["広告目的"].fillna("").str.contains("コンバージョン", case=False, na=False)
mask_cpa    = df["CPA"].notna()
mask_cpa_go = df["CPA_good"].notna()
mask_target = df["目標CPA"].notna()
df.loc[~mask_conv, "達成状況"] = "評価外"
mask_judge = mask_conv & mask_cpa
df.loc[mask_judge, "達成状況"] = "未達成"
df.loc[mask_judge & mask_cpa_go & (df["CPA"] <= df["CPA_good"]), "達成状況"] = "達成"
df.loc[mask_judge & mask_target & (df["CPA"] <= df["目標CPA"]),  "達成状況"] = "達成"

# ===== ここから表示用の補助関数（①対応をNaNも拾うよう強化） =====
def safe_cpa(cost, cv):
    return cost / cv if cv > 0 else np.nan

def fill_cpa_eval_for_display(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    表示専用：CV=0 かつ (CPA=0円 または CPAがNaN) かつ コンバージョン目的 かつ 評価が空/NaN → '✕' に置換
    """
    d = df_in.copy()
    if "CPA_KPI_評価" not in d.columns:
        return d
    is_conv   = d.get("広告目的", pd.Series(index=d.index)).fillna("").str.contains("コンバージョン", na=False)
    zero_cv   = d.get("コンバージョン数", pd.Series(index=d.index)).fillna(0).astype(float).eq(0)
    cpa_series = d.get("CPA", pd.Series(index=d.index, dtype="float"))
    zero_or_nan_cpa = cpa_series.isna() | cpa_series.fillna(0).astype(float).eq(0)
    blank_eval = d["CPA_KPI_評価"].isna() | (d["CPA_KPI_評価"].astype(str).str.strip() == "")
    d.loc[is_conv & zero_cv & zero_or_nan_cpa & blank_eval, "CPA_KPI_評価"] = "✕"
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

# UIの並び
f1, f2, f3, f4 = st.columns(4)
with f1:
    unit_filter = st.multiselect("🏷️ Unit", unit_options, placeholder="すべて")
with f2:
    person_filter = st.multiselect("👤 担当者", person_options, placeholder="すべて")
with f3:
    front_filter = st.multiselect("👤 フロント", front_options, placeholder="すべて")
with f4:
    default_employment = [x for x in employment_options if x in ["社員", "インターン"]]
    employment_filter = st.multiselect(
        "🏢 雇用形態", employment_options, default=default_employment, key="employment_type"
    )

f5, f6, f7 = st.columns(3)
with f5:
    focus_filter = st.multiselect("📌 注力度", focus_options, placeholder="すべて")
with f6:
    maincat_filter = st.multiselect("📁 メインカテゴリ", maincat_options, placeholder="すべて")
with f7:
    subcat_filter = st.multiselect("📂 サブカテゴリ", subcat_options, placeholder="すべて")

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

# --- フィルター適用（複数選択対応）
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

unit_summary_df = pd.DataFrame(unit_summary).sort_values("所属")

# --- Unit別色マップ
unit_colors = ["#c0e4eb", "#cbebb5", "#ffdda6"]
unit_color_map = {unit: unit_colors[i % len(unit_colors)] for i, unit in enumerate(unit_summary_df["所属"].unique())}

# --- Unitカード ---
st.write("#### 🍋🍋‍🟩 Unitごとのスコア 🍒🍏")
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
person_summary_df = pd.DataFrame(person_summary).sort_values("担当者")
person_summary_df = person_summary_df.merge(
    latest[["担当者", "所属"]].drop_duplicates(), on="担当者", how="left"
)

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
    unit_agg["達成率"] = unit_agg["達成件数"] / unit_agg["campaign_count"]
    unit_agg = unit_agg.sort_values("達成率", ascending=False)
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

# ① 表示専用の評価補正（CV=0 & (CPA=0 or NaN) & コンバージョン目的 & 評価空→'✕'）
display_df_disp = fill_cpa_eval_for_display(display_df)

# ▼ キャンペーン固有ID順に並び替え（昇順）
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

        # ① 表示専用の評価補正
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

    # ←← 修正ポイントその1：達成状況に依存せず、まず「コンバージョン目的」全量から抽出
    conv_base = df_filtered[df_filtered["広告目的"].fillna("").str.contains("コンバージョン", na=False)]

    # ←← 修正ポイントその2：CPA_KPI_評価 が「✕」または「空白（NaN/空文字）」を表示対象に
    if "CPA_KPI_評価" in conv_base.columns:
        is_blank = conv_base["CPA_KPI_評価"].isna() | (conv_base["CPA_KPI_評価"].astype(str).str.strip() == "")
        missed = conv_base[ conv_base["CPA_KPI_評価"].eq("✕") | is_blank ]
    else:
        missed = conv_base.copy()

    if not missed.empty:
        cols = [
            "配信月", "キャンペーン名", "担当者", "所属",
            "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"
        ]
        display_cols = [c for c in cols if c in missed.columns]

        # ① 表示専用の評価補正（NaN CPAも拾って✕にする）
        missed_disp = fill_cpa_eval_for_display(missed[display_cols])

        st.dataframe(
            missed_disp.style.format({
                "CPA": "¥{:,.0f}",
                "目標CPA": "¥{:,.0f}"
            }),
            use_container_width=True
        )
    else:
        st.info("未達成キャンペーンがありません。")

    st.markdown("<div style='margin-top: 1.2rem;'></div>", unsafe_allow_html=True)

    # ③ 新規：評価外キャンペーン一覧（CPA_KPI_評価 == '評価外'）
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
