import streamlit as st
import pandas as pd
from google.cloud import bigquery
import re
import html

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
#    バナー指標         … Banner_Drive_Ready
# ──────────────────────────────────────────────
df_num = bq.query(
    "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data_Last`"
).to_dataframe()

df_banner = bq.query(
    "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Banner_Drive_Ready`"
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
    "CV": "conv_banner"                  # バナー別 CV
})

# 数値型を明示
for col in ("conv_total", "conv_banner"):
    if col in df_num.columns:
        df_num[col] = pd.to_numeric(df_num[col], errors="coerce")
    if col in df_banner.columns:
        df_banner[col] = pd.to_numeric(df_banner[col], errors="coerce")

# 配信月は “YYYY/MM” 文字列
for d in (df_num, df_banner):
    if "配信月" in d.columns:
        d["配信月"] = d["配信月"].astype(str)

# ──────────────────────────────────────────────
# ③ フィルター UI  ※df_num 基準でマスタ値を取得
# ──────────────────────────────────────────────
st.markdown("<h3 class='top'>🔎 広告を絞り込む</h3>", unsafe_allow_html=True)

# 最初は全データ
filtered = df_num.copy()

# --- 1段目: 配信月 & クライアント名 ---
col1, col2 = st.columns(2)
with col1:
    month_options = sorted(filtered["配信月"].dropna().unique())
    sel_month = st.multiselect("📅 配信月", month_options, placeholder="すべて")
    if sel_month:
        filtered = filtered[filtered["配信月"].isin(sel_month)]
with col2:
    client_options = sorted(filtered["client_name"].dropna().unique())
    sel_client = st.multiselect("👤 クライアント名", client_options, placeholder="すべて")
    if sel_client:
        filtered = filtered[filtered["client_name"].isin(sel_client)]

# --- 2段目: カテゴリ・媒体・広告目的 ---
col3, col4, col5 = st.columns(3)
with col3:
    cat_options = sorted(filtered["カテゴリ"].dropna().unique())
    sel_cat = st.multiselect("📁 カテゴリ", cat_options, placeholder="すべて")
    if sel_cat:
        filtered = filtered[filtered["カテゴリ"].isin(sel_cat)]
with col4:
    media_options = sorted(filtered["ServiceNameJA"].dropna().unique())
    sel_media = st.multiselect("📡 媒体", media_options, placeholder="すべて")
    if sel_media:
        filtered = filtered[filtered["ServiceNameJA"].isin(sel_media)]
with col5:
    goal_options = sorted(filtered["広告目的"].dropna().unique())
    sel_goal = st.multiselect("🎯 広告目的", goal_options, placeholder="すべて")
    if sel_goal:
        filtered = filtered[filtered["広告目的"].isin(sel_goal)]

# --- 下段: キャンペーン名 ---
camp_options = sorted(filtered["キャンペーン名"].dropna().unique())
sel_campaign = st.multiselect("📣 キャンペーン名", camp_options, placeholder="すべて")
if sel_campaign:
    filtered = filtered[filtered["キャンペーン名"].isin(sel_campaign)]

# 以降、filteredを df_num_filt として以降で使う
df_num_filt = filtered


# ──────────────────────────────────────────────
# ④ フィルター関数（キャンペーン / バナー両方へ適用）
# ──────────────────────────────────────────────
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    cond = pd.Series(True, index=df.index)
    if "client_name" in df.columns and sel_client:
        cond &= df["client_name"].isin(sel_client)
    if "配信月" in df.columns and sel_month:
        cond &= df["配信月"].isin(sel_month)
    if "カテゴリ" in df.columns and sel_cat:
        cond &= df["カテゴリ"].isin(sel_cat)
    if "広告目的" in df.columns and sel_goal:
        cond &= df["広告目的"].isin(sel_goal)
    if "ServiceNameJA" in df.columns and sel_media:
        cond &= df["ServiceNameJA"].isin(sel_media)
    if "キャンペーン名" in df.columns and sel_campaign:
        cond &= df["キャンペーン名"].isin(sel_campaign)
    return df.loc[cond].copy()

df_num_filt    = apply_filters(df_num)
df_banner_filt = apply_filters(df_banner)

# バナーは画像 URL がある行だけを軽量表示 (最大 100 件)
df_banner_disp = df_banner_filt[df_banner_filt["CloudStorageUrl"].notna()].head(100) if "CloudStorageUrl" in df_banner_filt.columns else df_banner_filt.head(100)

# ──────────────────────────────────────────────
# ⑤ KPI スコアカード（キャンペーン単位）
# ──────────────────────────────────────────────
total_cost  = df_num_filt["Cost"].sum() if "Cost" in df_num_filt.columns else 0
total_click = df_num_filt["Clicks"].sum() if "Clicks" in df_num_filt.columns else 0
total_cv    = df_num_filt["conv_total"].sum() if "conv_total" in df_num_filt.columns else 0
total_imp   = df_num_filt["Impressions"].sum() if "Impressions" in df_num_filt.columns else 0

cpa = total_cost / total_cv if total_cv else None
cvr = total_cv / total_click if total_click else None
ctr = total_click / total_imp if total_imp else None
cpm = (total_cost * 1000 / total_imp) if total_imp else None

# 配信月：空やNaN対策
if "配信月" not in df_num_filt.columns or df_num_filt["配信月"].dropna().empty:
    delivery_range = "-"
else:
    delivery_range = f"{df_num_filt['配信月'].dropna().min()} 〜 {df_num_filt['配信月'].dropna().max()}"

st.markdown(
    f"📅 配信月：{delivery_range}　"
    f"👤 クライアント：{sel_client or 'すべて'}<br>"
    f"📁 カテゴリ：{sel_cat or 'すべて'}　"
    f"📡 媒体：{sel_media or 'すべて'}　"
    f"🎯 広告目的：{sel_goal or 'すべて'}<br>"
    f"📣 キャンペーン名：{sel_campaign or 'すべて'}",
    unsafe_allow_html=True
)


# ──────────────────────────────────────────────
# ⑥ 広告数値
# ──────────────────────────────────────────────
st.subheader("💠 広告数値")
# 3列（上段）
row1 = [
    {"label": "CPA - 獲得単価", "value": f"{cpa:,.0f}円" if cpa else "-", "bg": "#fff"},
    {"label": "コンバージョン数", "value": f"{int(total_cv):,}", "bg": "#fff"},
    {"label": "CVR - コンバージョン率", "value": f"{cvr*100:,.2f}%" if cvr else "-", "bg": "#fff"},
]
cols1 = st.columns(5)
for i, card in enumerate(row1):
    with cols1[i]:
        st.markdown(f"""
            <div class="scorecard" style="
                background:{card['bg']};
                border-radius: 11px;
                padding: 1.1rem .8rem .8rem .8rem;
                margin-bottom: 0.8rem;
                box-shadow: 0 2px 6px rgba(50,60,80,.04);
                border:1px solid #e4e4e4;">
              <div class="scorecard-label" style="font-size:14px; color:#111; margin-bottom:2px;">
                {card['label']}
              </div>
              <div class="scorecard-value" style="font-size:2.0rem; font-weight:600; color:#111; letter-spacing:0.01em;">
                {card['value']}
              </div>
            </div>
        """, unsafe_allow_html=True)

# 5列（下段）
row2 = [
    {"label": "消化金額", "value": f"{total_cost:,.0f}円", "bg": "#fff"},
    {"label": "インプレッション", "value": f"{int(total_imp):,}", "bg": "#fff"},
    {"label": "CTR - クリック率", "value": f"{ctr*100:,.2f}%" if ctr else "-", "bg": "#fff"},
    {"label": "CPM", "value": f"{cpm:,.0f}" if cpm else "-", "bg": "#fff"},
    {"label": "クリック", "value": f"{int(total_click):,}", "bg": "#fff"},
]
cols2 = st.columns(5)
for i, card in enumerate(row2):
    with cols2[i]:
        st.markdown(f"""
            <div class="scorecard" style="
                background:{card['bg']};
                border-radius: 11px;
                padding: 1.1rem .8rem .8rem .8rem;
                margin-bottom: 0.8rem;
                box-shadow: 0 2px 6px rgba(50,60,80,.04);
                border:1px solid #e4e4e4;">
              <div class="scorecard-label" style="font-size:14px; color:#111; margin-bottom:2px;">
                {card['label']}
              </div>
              <div class="scorecard-value" style="font-size:2.0rem; font-weight:600; color:#111; letter-spacing:0.01em;">
                {card['value']}
              </div>
            </div>
        """, unsafe_allow_html=True)

# ──────────────────────────────────────────────
# ⑦ バナー並び替え UI
# ──────────────────────────────────────────────
st.subheader("💠 配信バナー")
order = st.radio("並び替え基準", ["広告番号順", "CV数の多い順", "CPAの低い順"])

df_banner_sorted = df_banner_filt.copy()
if order == "CV数の多い順":
    df_banner_sorted = df_banner_sorted.sort_values("conv_banner", ascending=False)
elif order == "CPAの低い順":
    df_banner_sorted = df_banner_sorted[df_banner_sorted["CPA"].notna()].sort_values("CPA")
else:  # 広告番号順
    if "banner_number" in df_banner_sorted.columns:
        df_banner_sorted = df_banner_sorted.sort_values("banner_number")

# 「ソートした後で」CloudStorageUrlのある上位100件のみ表示
df_banner_disp = df_banner_sorted[df_banner_sorted["CloudStorageUrl"].notna()].head(100)


# ──────────────────────────────────────────────
# ⑧ バナーカード描画
# ──────────────────────────────────────────────
def split_urls(raw):
    urls = re.split(r"[,\s　]+", str(raw or ""))
    urls = [u.strip() for u in urls if u.strip().startswith("http")]
    return urls

cols = st.columns(5, gap="small")
for i, (_, row) in enumerate(df_banner_disp.iterrows()):
    cost = row.get("Cost", 0)
    imp  = row.get("Impressions", 0)
    clk  = row.get("Clicks", 0)
    cv   = int(row.get("conv_banner", 0)) if pd.notna(row.get("conv_banner", 0)) else 0
    cpa_ = row.get("CPA")
    ctr_ = row.get("CTR")

    canva_links = split_urls(row.get("canvaURL", ""))
    if canva_links:
        canva_html = "<br>".join(
            f'<a href="{html.escape(u)}" target="_blank">canvaURL{"↗️" if j == 0 else str(j+1)+"↗️"}</a>'
            for j, u in enumerate(canva_links)
        )
    else:
        canva_html = '<span class="gray-text">canvaURL：なし</span>'

    caption = [
        f"<div style='font-size:10px;color:#888;margin-bottom:-17px;line-height:1.4;'>{row.get('キャンペーン名','')}</div>",
        f"<b>広告名：</b>{row.get('AdName', '')}",
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
        <a href="{row.get('CloudStorageUrl', '')}" target="_blank" rel="noopener">
          <img src="{row.get('CloudStorageUrl', '')}">
        </a>
        <div class='banner-caption'>{"<br>".join(caption)}</div>
      </div>
    """
    with cols[i % 5]:
        st.markdown(card_html, unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  フォント & CSS
# ──────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700&display=swap" rel="stylesheet">
""", unsafe_allow_html=True)

theme = st.get_option("theme.base")
is_dark = theme == "dark"

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
</style>
""", unsafe_allow_html=True)
