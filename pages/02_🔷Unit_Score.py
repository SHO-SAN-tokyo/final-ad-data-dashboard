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

# === 上部に折りたたみ（expander）で管理者操作 ===
with st.expander("🛠️ 広告数値の手動更新（管理者用・通常は触らないでOK）", expanded=False):
    st.warning("※ この操作は数分かかる場合あり、同時に何度も押さないでください。")
    URL_META = "https://asia-northeast1-careful-chess-406412.cloudfunctions.net/upload-sql-data"
    URL_GOOGLE = "https://asia-northeast1-careful-chess-406412.cloudfunctions.net/upload-sql-data-pmax"
    st.markdown(f"**Meta広告の数値を更新:**  \n[こちらをクリック（所要時間：1~2分）]({URL_META})", unsafe_allow_html=True)
    st.markdown(f"**Google広告の数値を更新:**  \n[こちらをクリック（所要時間：1分弱）]({URL_GOOGLE})", unsafe_allow_html=True)
    st.info("クリック後、画面に完了ログが表示されたら、一呼吸おいてキャッシュクリアボタンでを押して最新化してください。")

# --- キャッシュクリア ---
col1, col2 = st.columns([4, 1])
with col1:
    st.markdown(f"<h1 style='display:inline-block;margin-bottom:0;'>🔷 Unit Score</h1>", unsafe_allow_html=True)
with col2:
    if st.button("🧹 キャッシュクリア", key="refresh_btn"):
        st.cache_data.clear()
        st.session_state["just_cleared_cache"] = True
        st.experimental_rerun()

# --- 再読み込み後にメッセージ表示 ---
if st.session_state.get("just_cleared_cache"):
    st.success("✅ キャッシュをクリアしました！")
    st.session_state["just_cleared_cache"] = False  # メッセージは1回だけ


st.subheader("📊 広告TM パフォーマンス")

info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ取得（VIEW）
@st.cache_data(show_spinner="データ取得中…")
def load_data():
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Unit_Drive_Ready_View").to_dataframe()
    return df

df = load_data()

# 📅 配信月（multiselectに変更）
month_options = sorted(df["配信月"].dropna().unique())
sel_month = st.multiselect("📅 配信月", month_options, placeholder="すべて")
if sel_month:
    df = df[df["配信月"].isin(sel_month)]

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

# 👇 配信月＋キャンペーンID＋クライアント名で1行にまとめる
df_filtered = df_filtered.drop_duplicates(subset=["配信月", "CampaignId", "クライアント名"])

def safe_cpa(cost, cv):
    return cost / cv if cv > 0 else np.nan

# -----------------------------
# 1. Unitごとのサマリー（2軸）
# -----------------------------
def campaign_key(df):
    return df["配信月"].astype(str) + "_" + df["CampaignId"].astype(str) + "_" + df["クライアント名"].astype(str)

unit_group = df_filtered.groupby("所属", dropna=False)
unit_summary = []
for unit, group in unit_group:
    group_conv = group[group["広告目的"] == "コンバージョン"]
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
    # 「広告目的=コンバージョン」のみ
    group_conv = group[group["広告目的"] == "コンバージョン"]
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
    # コンバージョン目的のみで分母・分子を計算
    conv_df = df_filtered[df_filtered["広告目的"] == "コンバージョン"].copy()
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
    conv_df = df_filtered[df_filtered["広告目的"] == "コンバージョン"]
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

# ▼ キャンペーン一覧（必要なカラム全て追加＆整形）
st.write("#### 📋 配信キャンペーン一覧（最大1,000件）")
columns_to_show = [
    "配信月","キャンペーン名","担当者","所属","フロント","雇用形態",
    "予算","フィー","クライアント名","消化金額","canvaURL",
    "カテゴリ","媒体","広告目的",
    "コンバージョン数","CPA","CVR","CTR","CPC","CPM",
    "CPA_KPI_評価","個別CPA_達成","CTR_KPI_評価","CPC_KPI_評価","CPM_KPI_評価"
]
columns_to_show = [col for col in columns_to_show if col in df_filtered.columns]
styled_table = df_filtered[columns_to_show].head(1000).style.format({
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
    achieved = df_filtered[(df_filtered["達成状況"] == "達成") & (df_filtered["広告目的"] == "コンバージョン")]
    if not achieved.empty:
        st.dataframe(
            achieved[[
                "配信月", "キャンペーン名", "担当者", "所属",
                "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"
            ]].style.format({
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
    missed = df_filtered[(df_filtered["達成状況"] == "未達成") & (df_filtered["広告目的"] == "コンバージョン")]
    if not missed.empty:
        st.dataframe(
            missed[[
                "配信月", "キャンペーン名", "担当者", "所属",
                "CPA", "CPA_KPI_評価", "目標CPA", "個別CPA_達成"
            ]].style.format({
                "CPA": "¥{:,.0f}",
                "目標CPA": "¥{:,.0f}"
            }),
            use_container_width=True
        )
    else:
        st.info("未達成キャンペーンがありません。")
