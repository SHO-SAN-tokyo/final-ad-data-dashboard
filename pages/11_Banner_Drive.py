import streamlit as st
import pandas as pd
from google.cloud import bigquery
import re

# --- ページ設定 ---
st.set_page_config(page_title="🔸 Banner Drive", layout="wide")
st.title("🔸 Banner Drive")

# --- BigQuery 認証 ---
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(cred)

# --- データ取得 ---
query = """
SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Banner_Ready_Combined`
"""
df = client.query(query).to_dataframe()
if df.empty:
    st.warning("データが存在しません")
    st.stop()

# --- 前処理 ---
df["配信月"] = df["配信月"].astype(str)
df["カテゴリ"] = df["カテゴリ"].fillna("未設定")
df["配信月_dt"] = pd.to_datetime(df["配信月"] + "-01", errors="coerce")

# --- フィルター ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    sel_client = st.multiselect("クライアント名", sorted(df["client_name"].dropna().unique()))
with col2:
    sel_month = st.multiselect("配信月", sorted(df["配信月"].dropna().unique()))
with col3:
    sel_cat = st.multiselect("カテゴリ", sorted(df["カテゴリ"].dropna().unique()))
with col4:
    sel_goal = st.multiselect("広告目的", sorted(df["広告目的"].dropna().unique()))

sel_campaign = st.multiselect("キャンペーン名", sorted(df["キャンペーン名"].dropna().unique()))

if sel_client:
    df = df[df["client_name"].isin(sel_client)]
if sel_month:
    df = df[df["配信月"].isin(sel_month)]
if sel_cat:
    df = df[df["カテゴリ"].isin(sel_cat)]
if sel_goal:
    df = df[df["広告目的"].isin(sel_goal)]
if sel_campaign:
    df = df[df["キャンペーン名"].isin(sel_campaign)]

# --- フィルター後のDataFrameを2分割 ---
df_filtered = df.copy()
df_display = df[df["CloudStorageUrl"].notnull()].head(100)

# --- 絞り込み条件の表示 ---
st.markdown("### 🔎 選択中の絞り込み条件")
st.markdown(
    f"📅 配信月：{df_filtered['配信月'].min()} 〜 {df_filtered['配信月'].max()}　"
    f"👤 クライアント：{sel_client if sel_client else 'すべて'}　"
    f"📁 カテゴリ：{sel_cat if sel_cat else 'すべて'}　"
    f"📣 キャンペーン名：{sel_campaign if sel_campaign else 'すべて'}"
)

# --- スコアカード集計 ---
total_cost = df_filtered["Cost"].sum()
total_clicks = df_filtered["Clicks"].sum()
total_cv = df_filtered["cv_value"].sum()
total_impressions = df_filtered["Impressions"].sum()

cpa = total_cost / total_cv if total_cv else None
cvr = total_cv / total_clicks if total_clicks else None
ctr = total_clicks / total_impressions if total_impressions else None
cpm = (total_cost * 1000 / total_impressions) if total_impressions else None

# --- スコアカード表示 ---
st.markdown("### 📊 この絞り込み条件での広告パフォーマンス")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("#### CPA - 獲得単価")
    st.markdown(f"<div class='scorecard'>{cpa:,.0f}円</div>" if cpa else "<div class='scorecard'>-</div>", unsafe_allow_html=True)
with col2:
    st.markdown("#### コンバージョン数")
    st.markdown(f"<div class='scorecard'>{int(total_cv):,}</div>", unsafe_allow_html=True)
with col3:
    st.markdown("#### CVR - コンバージョン率")
    st.markdown(f"<div class='scorecard'>{cvr * 100:.2f}%</div>" if cvr else "<div class='scorecard'>-</div>", unsafe_allow_html=True)

col4, col5, col6, col7, col8 = st.columns(5)
with col4:
    st.markdown("#### 消化金額")
    st.markdown(f"<div class='scorecard'>{total_cost:,.0f}円</div>", unsafe_allow_html=True)
with col5:
    st.markdown("#### インプレッション")
    st.markdown(f"<div class='scorecard'>{int(total_impressions):,}</div>", unsafe_allow_html=True)
with col6:
    st.markdown("#### CTR - クリック率")
    st.markdown(f"<div class='scorecard'>{ctr * 100:.2f}%</div>" if ctr else "<div class='scorecard'>-</div>", unsafe_allow_html=True)
with col7:
    st.markdown("#### CPM")
    st.markdown(f"<div class='scorecard'>{cpm:,.0f}円</div>" if cpm else "<div class='scorecard'>-</div>", unsafe_allow_html=True)
with col8:
    st.markdown("#### クリック")
    st.markdown(f"<div class='scorecard'>{int(total_clicks):,}</div>", unsafe_allow_html=True)

# --- 並び順選択 ---
st.markdown("<div style='margin-top:3.5rem;'></div>", unsafe_allow_html=True)
st.subheader("💠配信バナー")
opt = st.radio("並び替え基準", ["広告番号順", "CV数の多い順", "CPAの低い順"])

if opt == "CV数の多い順":
    df_display = df_display[df_display["cv_value"] > 0].sort_values("cv_value", ascending=False)
elif opt == "CPAの低い順":
    df_display = df_display[df_display["CPA"].notna()].sort_values("CPA")
else:
    df_display = df_display.sort_values("banner_number")

# --- バナー表示 ---
def urls(raw):
    return [u for u in re.split(r"[,\\s]+", str(raw or "")) if u.startswith("http")]

cols = st.columns(5, gap="small")
for i, (_, r) in enumerate(df_display.iterrows()):
    cost = r.get("Cost", 0)
    imp = r.get("Impressions", 0)
    clk = r.get("Clicks", 0)
    cv = int(r["cv_value"]) if pd.notna(r["cv_value"]) else 0
    cpa = r.get("CPA")
    ctr = r.get("CTR")
    text = r.get("Description1ByAdType", "")

    lnks = urls(r.get("canvaURL", ""))
    canva_html = (" ,".join(
        f'<a href="{u}" target="_blank">canvaURL{i+1 if len(lnks)>1 else ""}↗️</a>'
        for i, u in enumerate(lnks))
        if lnks else '<span class="gray-text">canvaURL：なし❌</span>'
    )

    caption = [
        f"<b>広告名：</b>{r['AdName']}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{ctr*100:.2f}%" if pd.notna(ctr) else "<b>CTR：</b>-",
        f"<b>CV数：</b>{cv if cv else 'なし'}",
        f"<b>CPA：</b>{cpa:.0f}円" if pd.notna(cpa) else "<b>CPA：</b>-",
        canva_html,
        f"<b>メインテキスト：</b>{text}"
    ]

    card_html = f"""
      <div class='banner-card'>
        <a href="{r['CloudStorageUrl']}" target="_blank" rel="noopener">
          <img src="{r['CloudStorageUrl']}">
        </a>
        <div class='banner-caption'>{"<br>".join(caption)}</div>
      </div>
    """
    with cols[i % 5]:
        st.markdown(card_html, unsafe_allow_html=True)

# --- CSS ---
st.markdown("""
    <style>
      .scorecard {
        background-color: #f5f5f5;
        color: #333;
        font-weight: bold;
        font-size: 28px;
        padding: 10px 20px;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 10px;
        box-shadow: 1px 1px 5px rgba(0,0,0,0.05);
      }
      .banner-card {
        padding:12px 12px 20px;
        border:1px solid #e6e6e6;
        border-radius:12px;
        background:#fafafa;
        height:100%;
        margin-bottom:14px;
      }
      .banner-card img {
        width:100%;
        height:203px;
        object-fit:cover;
        border-radius:8px;
        cursor:pointer;
      }
      .banner-caption {
        margin-top:8px;
        font-size:14px;
        line-height:1.6;
        text-align:left;
      }
      .gray-text { color:#888; }
    </style>
""", unsafe_allow_html=True)
