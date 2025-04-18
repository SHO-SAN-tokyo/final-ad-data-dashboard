# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd, numpy as np, re

# ---------- 0. 画面設定 & CSS ----------
st.set_page_config(page_title="配信バナー", layout="wide")
st.markdown("""
<style>
 .banner-card{padding:12px 12px 20px;border:1px solid #e6e6e6;border-radius:12px;
              background:#fafafa;height:100%;margin-bottom:14px;}
 .banner-card img{width:100%;height:180px;object-fit:cover;border-radius:8px;cursor:pointer;}
 .banner-caption{margin-top:8px;font-size:14px;line-height:1.6;text-align:left;}
 .gray-text{color:#888;}
</style>
""", unsafe_allow_html=True)
st.title("🖼️ 配信バナー")

# ---------- 1. BigQuery 取得 ----------
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq   = bigquery.Client.from_service_account_info(cred)

query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
with st.spinner("🔄 データを取得中..."):
    df = bq.query(query).to_dataframe()
if df.empty: st.warning("⚠️ データがありません"); st.stop()

# ---------- 共通前処理 ----------
df["カテゴリ"] = df.get("カテゴリ","").astype(str).str.strip().replace("", "未設定").fillna("未設定")
df["Date"]     = pd.to_datetime(df.get("Date"), errors="coerce")

# ---------- 日付フィルタ ----------
if not df["Date"].isnull().all():
    dmin,dmax = df["Date"].min().date(), df["Date"].max().date()
    sel = st.sidebar.date_input("日付フィルター",(dmin,dmax),min_value=dmin,max_value=dmax)
    if isinstance(sel,(list,tuple)) and len(sel)==2:
        s,e=map(pd.to_datetime,sel)
        df=df[(df["Date"].dt.date>=s.date())&(df["Date"].dt.date<=e.date())]
    else:
        d=pd.to_datetime(sel).date(); df=df[df["Date"].dt.date==d]

# ---------- サイドバー絞り込み ----------
st.sidebar.header("🔍 フィルター")
def _select(label,col):
    opts=["すべて"]+sorted(df[col].dropna().unique())
    sel=st.sidebar.selectbox(label,opts)
    return None if sel=="すべて" else sel
for c,lbl in [("PromotionName","クライアント"),("カテゴリ","カテゴリ"),("CampaignName","キャンペーン名")]:
    v=_select(lbl,c)
    if v is not None: df=df[df[c]==v]

# ---------- 1〜60 列補完 ----------
for i in range(1,61):
    col=str(i)
    df[col]=pd.to_numeric(df.get(col,0), errors="coerce").fillna(0)

# ======================================================================
# ★ A. 数値サマリー表 ★
# ======================================================================
# 数値化
df["Cost"]           = pd.to_numeric(df.get("Cost"), errors="coerce")
df["Impressions"]    = pd.to_numeric(df.get("Impressions"), errors="coerce")
df["Clicks"]         = pd.to_numeric(df.get("Clicks"), errors="coerce")
df["コンバージョン数"]= pd.to_numeric(df.get("コンバージョン数"), errors="coerce")
df["Reach"]          = pd.to_numeric(df.get("Reach"), errors="coerce")

# ① 期間合計（Cost / Imp / Click）
tot_cost  = df["Cost"].sum(skipna=True)
tot_imp   = df["Impressions"].sum(skipna=True)
tot_clk   = df["Clicks"].sum(skipna=True)

# ② Ad 単位で「最新行」のコンバージョン数を取得して合計
latest_conv = (df.dropna(subset=["Date"])
                .sort_values("Date")
                .loc[lambda d: d.groupby(["CampaignId","AdName"])["Date"].idxmax()]
                ["コンバージョン数"].fillna(0))
tot_conv = latest_conv.sum()

# ③ Reach 合計
tot_reach = df["Reach"].sum(skipna=True)

def div(n,d): return np.nan if (d==0 or pd.isna(d)) else n/d

summary = pd.DataFrame({
    "指標": ["CPA","コンバージョン数","CVR","消化金額","インプレッション",
           "CTR","CPC","クリック数","CPM","フリークエンシー"],
    "値": [
        div(tot_cost,tot_conv),            # CPA
        tot_conv,
        div(tot_conv,tot_clk),             # CVR
        tot_cost,
        tot_imp,
        div(tot_clk,tot_imp),              # CTR
        div(tot_cost,tot_clk),             # CPC
        tot_clk,
        div(tot_cost*1000,tot_imp),        # CPM
        div(tot_imp,tot_reach)             # Freq
    ],
    "単位": ["円","","%","円","","%","円","","円",""]
})

# 小数は不要：四捨五入→整数
def fmt(v,u):
    if pd.isna(v): return "-"
    v=round(v)
    return f"{v:,.0f}{u}" if u else f"{v:,.0f}"
summary["値"] = summary.apply(lambda r: fmt(r["値"],r["単位"]),axis=1)
summary.drop(columns="単位",inplace=True)

st.subheader("📊 集計サマリー")
st.table(summary)

# ======================================================================
# B. 画像バナー表示（ロジック変更なし・表示順だけ CPA 修正）
# ======================================================================
st.subheader("🌟並び替え")

img_src=df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
if img_src.empty: st.warning("⚠️ 表示できる画像がありません"); st.stop()

img_src["AdName"]     = img_src["AdName"].astype(str).str.strip()
img_src["CampaignId"] = img_src["CampaignId"].astype(str).str.strip()
img_src["AdNum"]      = pd.to_numeric(img_src["AdName"], errors="coerce")

# Ad 単位で最新行のみ
latest = (img_src.dropna(subset=["Date"])
          .sort_values("Date")
          .loc[lambda d: d.groupby(["CampaignId","AdName"])["Date"].idxmax()]
          .copy())

# 最新行で CV を計算
def row_cv(r):
    n=r["AdNum"]; col=str(int(n)) if pd.notna(n) else None
    return r[col] if col and col in r and isinstance(r[col],(int,float)) else 0
latest["CV件数"]=latest.apply(row_cv,axis=1)

# 最新行で CPA を計算（Cost / CV件数）
latest["CPA"] = latest.apply(lambda r: div(r["Cost"], r["CV件数"]), axis=1)

# 並び替え
opt=st.radio("並び替え基準",["広告番号順","コンバージョン数の多い順","CPAの低い順"])
if opt=="コンバージョン数の多い順":
    latest=latest[latest["CV件数"]>0].sort_values("CV件数",ascending=False)
elif opt=="CPAの低い順":
    latest=latest[latest["CPA"].notna()].sort_values("CPA")
else:
    latest=latest.sort_values("AdNum")

# 表示用マップ（Cost, Imp, Click は期間合計）
agg=(df.assign(AdName=lambda d:d["AdName"].astype(str).str.strip(),
               CampaignId=lambda d:d["CampaignId"].astype(str).str.strip())
      .groupby(["CampaignId","AdName"])
      .agg({"Cost":"sum","Impressions":"sum","Clicks":"sum"})
      .reset_index())
sum_map = agg.set_index(["CampaignId","AdName"]).to_dict("index")

cols=st.columns(5,gap="small")
def canva_links(raw):
    return [u for u in re.split(r'[,\s]+', str(raw or "")) if u.startswith("http")]

for i,(_,r) in enumerate(latest.iterrows()):
    key=(r["CampaignId"],r["AdName"])
    s=sum_map.get(key,{})
    cost,imp,clk=s.get("Cost",0),s.get("Impressions",0),s.get("Clicks",0)
    cv  = int(r["CV件数"])
    cpa = round(r["CPA"]) if pd.notna(r["CPA"]) else np.nan
    ctr = div(clk,imp)
    text= r.get("Description1ByAdType","")

    links=canva_links(r.get("canvaURL",""))
    canva_html=(" ,".join(f'<a href="{u}" target="_blank">canvaURL{i+1 if len(links)>1 else ""}↗️</a>'
                           for i,u in enumerate(links))
                if links else '<span class="gray-text">canvaURL：なし✖</span>')

    caption="<br>".join([
        f"<b>広告名：</b>{r['AdName']}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{round(ctr*100):,}%") if ctr is not np.nan else "<b>CTR：</b>-",
        f"<b>CV数：</b>{cv if cv else 'なし'}",
        f"<b>CPA：</b>{cpa:,.0f}円" if pd.notna(cpa) else "<b>CPA：</b>-",
        canva_html,
        f"<b>メインテキスト：</b>{text}",
    ])

    card=f"""
      <div class='banner-card'>
        <a href="{r['CloudStorageUrl']}" target="_blank" rel="noopener">
          <img src="{r['CloudStorageUrl']}">
        </a>
        <div class='banner-caption'>{caption}</div>
      </div>
    """
    with cols[i%5]:
        st.markdown(card, unsafe_allow_html=True)
