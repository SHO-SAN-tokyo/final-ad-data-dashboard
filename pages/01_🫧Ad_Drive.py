import streamlit as st
import pandas as pd
from google.cloud import bigquery
import re

# ──────────────────────────────────────────────
# ページ設定 & BigQuery 認証
# ──────────────────────────────────────────────
st.set_page_config(page_title="🫧 Ad Drive", layout="wide")
st.title("🫧 Ad Drive")

cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

# ──────────────────────────────────────────────
# ① データ取得
#    キャンペーン指標   … Final_Ad_Data_Last
#    バナー指標         … CV_List_Banner
# ──────────────────────────────────────────────
df_num = bq.query(
    "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data_Last`"
).to_dataframe()

df_banner = bq.query(
    "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.CV_List_Banner`"
).to_dataframe()

if df_num.empty and df_banner.empty:
    st.warning("データが存在しません")
    st.stop()

# ──────────────────────────────────────────────
# ② 前処理／列リネーム（※CV 列を分離）
# ──────────────────────────────────────────────
rename_common = {
    "媒体": "ServiceNameJA",
    "クライアント名": "client_name"
}

df_num = df_num.rename(columns={
    **rename_common,
    "コンバージョン数": "conv_total"      # キャンペーン総 CV
})
df_banner = df_banner.rename(columns={
    **rename_common,
    "コンバージョン数": "conv_banner"    # バナー別 CV
})

# 数値型を明示
for col in ("conv_total", "conv_banner"):
    if col in df_num.columns:
        df_num[col] = pd.to_numeric(df_num[col], errors="coerce")
    if col in df_banner.columns:
        df_banner[col] = pd.to_numeric(df_banner[col], errors="coerce")

# 配信月は “YYYY/MM” 文字列
for d in (df_num, df_banner):
    d["配信月"] = d["配信月"].astype(str)

# ──────────────────────────────────────────────
# ③ フィルター UI  ※df_num 基準でマスタ値を取得
# ──────────────────────────────────────────────
st.markdown("<h3 class='top'>🔎 広告を絞り込む</h3>", unsafe_allow_html=True)

col1, col2 = st.columns(2)
with col1:
    sel_month = st.multiselect("📅 配信月", sorted(df_num["配信月"].unique()), placeholder="すべて")
with col2:
    sel_client = st.multiselect("👤 クライアント名", sorted(df_num["client_name"].dropna().unique()), placeholder="すべて")

col3, col4, col5 = st.columns(3)
with col3:
    sel_cat = st.multiselect("📁 カテゴリ", sorted(df_num["カテゴリ"].dropna().unique()), placeholder="すべて")
with col4:
    sel_media = st.multiselect("📡 媒体", sorted(df_num["ServiceNameJA"].dropna().unique()), placeholder="すべて")
with col5:
    sel_goal = st.multiselect("🎯 広告目的", sorted(df_num["広告目的"].dropna().unique()), placeholder="すべて")

sel_campaign = st.multiselect("📣 キャンペーン名", sorted(df_num["キャンペーン名"].dropna().unique()), placeholder="すべて")

# ──────────────────────────────────────────────
# ④ フィルター関数（キャンペーン / バナー両方へ適用）
# ──────────────────────────────────────────────
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    cond = pd.Series(True, index=df.index)
    if sel_client:  cond &= df["client_name"].isin(sel_client)
    if sel_month:   cond &= df["配信月"].isin(sel_month)
    if sel_cat:     cond &= df["カテゴリ"].isin(sel_cat)
    if sel_goal:    cond &= df["広告目的"].isin(sel_goal)
    if sel_media:   cond &= df["ServiceNameJA"].isin(sel_media)
    if sel_campaign and "キャンペーン名" in df.columns:
        cond &= df["キャンペーン名"].isin(sel_campaign)
    return df.loc[cond].copy()

df_num_filt    = apply_filters(df_num)
df_banner_filt = apply_filters(df_banner)

# バナーは画像 URL がある行だけを軽量表示 (最大 100 件)
df_banner_disp = df_banner_filt[df_banner_filt["CloudStorageUrl"].notna()].head(100)

# ──────────────────────────────────────────────
# ⑤ KPI スコアカード（キャンペーン単位）
# ──────────────────────────────────────────────
total_cost  = df_num_filt["Cost"].sum()
total_click = df_num_filt["Clicks"].sum()
total_cv    = df_num_filt["conv_total"].sum()
total_imp   = df_num_filt["Impressions"].sum()

cpa = total_cost / total_cv if total_cv else None
cvr = total_cv / total_click if total_click else None
ctr = total_click / total_imp if total_imp else None
cpm = (total_cost * 1000 / total_imp) if total_imp else None

st.markdown("### 📊 広告パフォーマンス")
st.markdown(
    f"📅 配信月：{df_num_filt['配信月'].min()} 〜 {df_num_filt['配信月'].max()}　"
    f"👤 クライアント：{sel_client or 'すべて'}<br>"
    f"📁 カテゴリ：{sel_cat or 'すべて'}　"
    f"📡 媒体：{sel_media or 'すべて'}　"
    f"🎯 広告目的：{sel_goal or 'すべて'}<br>"
    f"📣 キャンペーン名：{sel_campaign or 'すべて'}",
    unsafe_allow_html=True
)

# 3+5 列のスコアカード
sc1, sc2, sc3, _, _ = st.columns(5)
with sc1: st.metric("CPA (円)", f"{cpa:,.0f}" if cpa else "-")
with sc2: st.metric("コンバージョン数", f"{int(total_cv):,}")
with sc3: st.metric("CVR (%)", f"{cvr*100:,.2f}" if cvr else "-")

sc4, sc5, sc6, sc7, sc8 = st.columns(5)
with sc4: st.metric("消化金額 (円)", f"{total_cost:,.0f}")
with sc5: st.metric("インプレッション", f"{int(total_imp):,}")
with sc6: st.metric("CTR (%)", f"{ctr*100:,.2f}" if ctr else "-")
with sc7: st.metric("CPM (円)", f"{cpm:,.0f}" if cpm else "-")
with sc8: st.metric("クリック", f"{int(total_click):,}")

# ──────────────────────────────────────────────
# ⑥ バナー並び替え UI
# ──────────────────────────────────────────────
st.subheader("💠 配信バナー")
order = st.radio("並び替え基準", ["広告番号順", "CV数の多い順", "CPAの低い順"])

if order == "CV数の多い順":
    df_banner_disp = df_banner_disp[df_banner_disp["conv_banner"] > 0]\
                     .sort_values("conv_banner", ascending=False)
elif order == "CPAの低い順":
    df_banner_disp = df_banner_disp[df_banner_disp["CPA"].notna()]\
                     .sort_values("CPA")
else:
    df_banner_disp = df_banner_disp.sort_values("banner_number")

# ──────────────────────────────────────────────
# ⑦ バナーカード描画
# ──────────────────────────────────────────────
def split_urls(raw):  # canvaURL / URL の複数リンク分割
    return [u for u in re.split(r"[,\\s]+", str(raw or "")) if u.startswith("http")]

cols = st.columns(5, gap="small")
for i, (_, row) in enumerate(df_banner_disp.iterrows()):
    cost = row["Cost"];     imp  = row["Impressions"]; clk = row["Clicks"]
    cv   = int(row["conv_banner"]) if pd.notna(row["conv_banner"]) else 0
    cpa_ = row["CPA"];      ctr_ = row["CTR"]

    canva_links = split_urls(row["canvaURL"])
    canva_html = " ,".join(
        f'<a href="{u}" target="_blank">canvaURL{j+1 if len(canva_links)>1 else ""}↗️</a>'
        for j, u in enumerate(canva_links)
    ) if canva_links else '<span class="gray-text">canvaURL：なし</span>'

    caption = [
        f"<b>広告名：</b>{row['AdName']}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{ctr_*100:.2f}%" if pd.notna(ctr_) else "<b>CTR：</b>-",
        f"<b>CV数：</b>{cv if cv else 'なし'}",
        f"<b>CPA：</b>{cpa_:,.0f}円" if pd.notna(cpa_) else "<b>CPA：</b>-",
        canva_html,
        f"<b>メインテキスト：</b>{row.get('Description', '')}"
    ]

    card_html = f"""
      <div class='banner-card'>
        <a href="{row['CloudStorageUrl']}" target="_blank" rel="noopener">
          <img src="{row['CloudStorageUrl']}">
        </a>
        <div class='banner-caption'>{"<br>".join(caption)}</div>
      </div>
    """
    with cols[i % 5]:
        st.markdown(card_html, unsafe_allow_html=True)

# ──────────────────────────────────────────────
# ⑧ フォント & CSS
# ──────────────────────────────────────────────
# --- Google Fontsを読み込む ---
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700&display=swap" rel="stylesheet">
""", unsafe_allow_html=True)

# --- CSS ---
theme = st.get_option("theme.base")
is_dark = theme == "dark"

# --- CSS 出力（ダーク or ライト）---
st.markdown("""
<style>
  h3.top {
    margin: .4rem auto 1rem auto !important;
  }
  h3 {
    background-color: #ddedfc;
    padding: .6rem !important;
    display: block;
    margin: 2rem auto 1rem auto !important;
  }
  .scorecard-label {
    font-size: 14px;
    margin-bottom: 6px;
    text-align: left;
    background: #f1f1f1;
    padding: .3rem .6rem;
  }

  .scorecard-value {
    font-size: 30px;
    text-align: left;
    line-height: 1.2;
    font-weight: 600;
    padding: 2px 10px;
    margin-bottom: 1.4rem;
    font-family: 'Inter', 'Roboto', sans-serif;
  }

  .banner-card {
    padding: 12px 12px 20px;
    border: 1px solid #e6e6e6;
    border-radius: 12px;
    background: #fafafa;
    height: auto;
    margin-bottom: 14px;
  }

  .banner-card img {
    width: 100%;
    height: auto;
    object-fit: contain;
    border-radius: 8px;
    cursor: pointer;
  }

  .banner-caption {
    margin-top: 8px;
    font-size: 14px;
    line-height: 1.6;
    text-align: left;
    color: #111;
    font-family: 'Inter', 'Roboto', sans-serif;
  }

  .gray-text {
    color: #888;
  }
  
  .st-emotion-cache-16tyu1 p {
    margin-bottom: 2rem;
  }

  /* 🌙 ダークモード対応 */
  @media (prefers-color-scheme: dark) {
    h3 {
      background-color: #394046;
    }
    .scorecard-label {
      color: #ccc !important;
    }
    .scorecard-value {
      background-color: #333 !important;
      color: #fff !important;
    }
    .banner-card {
      background: #222;
      border: 1px solid #444;
    }
    .banner-caption {
      color: #eee;
    }
    .gray-text {
      color: #aaa;
    }
  }
</style>
""", unsafe_allow_html=True)
