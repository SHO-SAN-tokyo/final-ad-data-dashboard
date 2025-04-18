# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import re

# ---------------- 0. ページ設定 & CSS ----------------
st.set_page_config(page_title="配信バナー", layout="wide")
st.markdown(
    """
    <style>
      .banner-card{
        padding:12px 12px 20px 12px;border:1px solid #e6e6e6;border-radius:12px;
        background:#fafafa;height:100%;margin-bottom:14px;
      }
      .banner-card img{
        width:100%;height:180px;object-fit:cover;border-radius:8px;cursor:pointer;
      }
      .banner-caption{margin-top:8px;font-size:14px;line-height:1.6;text-align:left;}
      .gray-text{color:#888;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("🖼️ 配信バナー")

# ---------------- 1. BigQuery から取得 ----------------
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(info)

query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
with st.spinner("🔄 データを取得中..."):
    df = bq.query(query).to_dataframe()

if df.empty:
    st.warning("⚠️ データがありません")
    st.stop()

# ---------- 前処理 ----------
df["カテゴリ"] = (
    df.get("カテゴリ", "")
      .astype(str).str.strip().replace("", "未設定").fillna("未設定")
)
df["Date"] = pd.to_datetime(df.get("Date"), errors="coerce")

# ---------- 日付フィルタ ----------
if not df["Date"].isnull().all():
    dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
    sel = st.sidebar.date_input("日付フィルター", (dmin, dmax), min_value=dmin, max_value=dmax)
    if isinstance(sel, (list, tuple)) and len(sel) == 2:
        s, e = map(pd.to_datetime, sel)
        df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]
    else:
        d = pd.to_datetime(sel).date()
        df = df[df["Date"].dt.date == d]

# ---------- サイドバー絞り込み ----------
st.sidebar.header("🔍 フィルター")
def sb_select(label, ser):
    opts = ["すべて"] + sorted(ser.dropna().unique())
    choice = st.sidebar.selectbox(label, opts)
    return None if choice == "すべて" else choice

if (c := sb_select("クライアント", df["PromotionName"])): df = df[df["PromotionName"] == c]
if (c := sb_select("カテゴリ",   df["カテゴリ"])):        df = df[df["カテゴリ"] == c]
if (c := sb_select("キャンペーン名", df["CampaignName"])): df = df[df["CampaignName"] == c]

# ---------- 1〜60 列を補完 ----------
for i in range(1, 61):
    col = str(i)
    df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

# ---------------- 2. 画像バナー ----------------
img_df = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
if img_df.empty:
    st.warning("⚠️ 表示できる画像がありません")
    st.stop()

img_df["AdName"]     = img_df["AdName"].astype(str).str.strip()
img_df["CampaignId"] = img_df["CampaignId"].astype(str).str.strip()
img_df["AdNum"]      = pd.to_numeric(img_df["AdName"], errors="coerce")

# CV 件数（行単位）
def row_cv(r):
    n = r["AdNum"]
    if pd.isna(n): return 0
    col = str(int(n))
    return r[col] if col in r and isinstance(r[col], (int, float)) else 0
img_df["CV件数"] = img_df.apply(row_cv, axis=1)

# ---- ★最新行を CampaignId + AdName で抽出★ ----
latest = (
    img_df.sort_values("Date")
          .dropna(subset=["Date"])
          .loc[lambda d: d.groupby(["CampaignId", "AdName"])["Date"].idxmax()]
          .copy()
)
latest["CTR"] = latest["Clicks"] / latest["Impressions"]
latest["CPA"] = latest.apply(
    lambda r: r["Cost"] / r["CV件数"] if r["CV件数"] > 0 else pd.NA, axis=1
)
metric = latest.set_index(["CampaignId", "AdName"]).to_dict("index")

# CV件数・CPA を img_df に付加（並び替え用）
img_df = img_df.merge(
    latest[["CampaignId", "AdName", "CV件数", "CPA"]],
    on=["CampaignId", "AdName"], how="left"
)

# ---------- 並び替え ----------
st.subheader("🌟 並び替え")
opt = st.radio("並び替え基準", ["広告番号順", "コンバージョン数の多い順", "CPAの低い順"])
if opt == "コンバージョン数の多い順":
    img_df = img_df[img_df["CV件数"] > 0].sort_values("CV件数", ascending=False)
elif opt == "CPAの低い順":
    img_df = img_df[img_df["CPA"].notna()].sort_values("CPA")
else:
    img_df = img_df.sort_values("AdNum")

# ---------- 表示 ----------
cols = st.columns(5, gap="small")
def canva_links(raw):
    return [u for u in re.split(r'[,\s]+', str(raw or "")) if u.startswith("http")]

for i, (_, r) in enumerate(img_df.iterrows()):
    key = (r["CampaignId"], r["AdName"])
    m = metric.get(key, {})
    cost, imp, clk = m.get("Cost",0), m.get("Impressions",0), m.get("Clicks",0)
    ctr, cv, cpa   = m.get("CTR"), m.get("CV件数",0), m.get("CPA")
    text           = m.get("Description1ByAdType","")

    links = canva_links(r.get("canvaURL",""))
    canva_html = (
        ", ".join(f'<a href="{l}" target="_blank">canvaURL{i+1 if len(links)>1 else ""}↗️</a>'
                  for i,l in enumerate(links))
        if links else '<span class="gray-text">canvaURL：なし✖</span>'
    )

    caption = "<br>".join([
        f"<b>広告名：</b>{r['AdName']}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{ctr*100:.2f}%" if pd.notna(ctr) else "<b>CTR：</b>-",
        f"<b>CV数：</b>{int(cv) if cv else 'なし'}",
        f"<b>CPA：</b>{cpa:,.0f}円" if pd.notna(cpa) else "<b>CPA：</b>-",
        canva_html,
        f"<b>メインテキスト：</b>{text}",
    ])

    card = f"""
      <div class='banner-card'>
        <a href="{r['CloudStorageUrl']}" target="_blank" rel="noopener">
          <img src="{r['CloudStorageUrl']}">
        </a>
        <div class='banner-caption'>{caption}</div>
      </div>
    """
    with cols[i % 5]:
        st.markdown(card, unsafe_allow_html=True)
