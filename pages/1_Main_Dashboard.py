# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd, re, numpy as np

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
def sb(label,ser):
    opts=["すべて"]+sorted(ser.dropna().unique())
    sel=st.sidebar.selectbox(label,opts)
    return None if sel=="すべて" else sel
for col,label in [("PromotionName","クライアント"),("カテゴリ","カテゴリ"),("CampaignName","キャンペーン名")]:
    if (v:=sb(label,df[col])) is not None: df=df[df[col]==v]

# ---------- 1〜60 列補完 ----------
for i in range(1,61):
    col=str(i)
    df[col]=pd.to_numeric(df.get(col,0), errors="coerce").fillna(0)

# ======================================================================
# ★ A. 数値サマリー表  （画像バナーに影響なし）★
# ======================================================================
# 必要列を数値化
df["Cost"]           = pd.to_numeric(df.get("Cost"), errors="coerce")
df["Impressions"]    = pd.to_numeric(df.get("Impressions"), errors="coerce")
df["Clicks"]         = pd.to_numeric(df.get("Clicks"), errors="coerce")
df["コンバージョン数"]= pd.to_numeric(df.get("コンバージョン数"), errors="coerce")
df["Reach"]          = pd.to_numeric(df.get("Reach"), errors="coerce")

# 合計値
tot_cost  = df["Cost"].sum(skipna=True)
tot_imp   = df["Impressions"].sum(skipna=True)
tot_clk   = df["Clicks"].sum(skipna=True)
tot_conv  = df["コンバージョン数"].sum(skipna=True)
tot_reach = df["Reach"].sum(skipna=True)

def safe_div(n,d):
    return np.nan if (d==0 or pd.isna(d)) else n/d

summary = pd.DataFrame({
    "指標": ["CPA(円)","コンバージョン数","CVR","消化金額(円)","インプレッション","CTR","CPC(円)","クリック数","CPM(円)","フリークエンシー"],
    "値": [
        safe_div(tot_cost,tot_conv),
        tot_conv,
        safe_div(tot_conv,tot_clk),
        tot_cost,
        tot_imp,
        safe_div(tot_clk,tot_imp),
        safe_div(tot_cost,tot_clk),
        tot_clk,
        safe_div(tot_cost*1000,tot_imp),
        safe_div(tot_imp,tot_reach)
    ]
})

st.subheader("📊 集計サマリー")
st.table(summary.style.format({"値":"{:,}".format}).format({"値":lambda v:"-" if pd.isna(v) else f"{v:,.2f}" if isinstance(v,float) else f"{int(v):,}"}))

# ======================================================================
# B. 画像バナー表示　（以前のロジックそのまま）
# ======================================================================
st.subheader("🌟並び替え")

img_src=df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
if img_src.empty: st.warning("⚠️ 表示できる画像がありません"); st.stop()

img_src["AdName"]     = img_src["AdName"].astype(str).str.strip()
img_src["CampaignId"] = img_src["CampaignId"].astype(str).str.strip()
img_src["AdNum"]      = pd.to_numeric(img_src["AdName"], errors="coerce")

# 同 CampaignId × AdName で最新 1 行
latest = (img_src.dropna(subset=["Date"])
          .sort_values("Date")
          .loc[lambda d: d.groupby(["CampaignId","AdName"])["Date"].idxmax()]
          .copy())

def row_cv(r):
    n=r["AdNum"]; col=str(int(n)) if pd.notna(n) else None
    return r[col] if col and col in r and isinstance(r[col],(int,float)) else 0
latest["CV件数"]=latest.apply(row_cv,axis=1)

# 期間合計（Cost/Imp/Click）
agg=(df.assign(AdName=lambda d:d["AdName"].astype(str).str.strip(),
               CampaignId=lambda d:d["CampaignId"].astype(str).str.strip())
      .groupby(["CampaignId","AdName"])
      .agg({"Cost":"sum","Impressions":"sum","Clicks":"sum"})
      .reset_index())
sum_map   = agg.set_index(["CampaignId","AdName"]).to_dict("index")
latest_map= latest.set_index(["CampaignId","AdName"])[["CV件数","Description1ByAdType"]].to_dict("index")
img_df    = latest.copy()

# 並び替え
opt=st.radio("並び替え基準",["広告番号順","コンバージョン数の多い順","CPAの低い順"])
if opt=="コンバージョン数の多い順":
    img_df=img_df[img_df["CV件数"]>0].sort_values("CV件数",ascending=False)
elif opt=="CPAの低い順":
    def _cpa(r):
        cost=sum_map.get((r["CampaignId"],r["AdName"]),{}).get("Cost",0)
        return cost/r["CV件数"] if r["CV件数"] else np.nan
    img_df["_CPA"]=img_df.apply(_cpa,axis=1)
    img_df=img_df[img_df["_CPA"].notna()].sort_values("_CPA")
else:
    img_df=img_df.sort_values("AdNum")

# 表示
cols=st.columns(5,gap="small")
def canva_links(raw):
    return [u for u in re.split(r'[,\s]+', str(raw or "")) if u.startswith("http")]

for i,(_,r) in enumerate(img_df.iterrows()):
    key=(r["CampaignId"],r["AdName"])
    s=sum_map.get(key,{})
    l=latest_map.get(key,{})
    cost,imp,clk=s.get("Cost",0),s.get("Impressions",0),s.get("Clicks",0)
    cv=l.get("CV件数",0)
    cpa=safe_div(cost,cv)
    text=l.get("Description1ByAdType","")

    links=canva_links(r.get("canvaURL",""))
    canva_html=(" ,".join(f'<a href="{u}" target="_blank">canvaURL{i+1 if len(links)>1 else ""}↗️</a>'
                           for i,u in enumerate(links))
                if links else '<span class="gray-text">canvaURL：なし✖</span>')

    caption="<br>".join([
        f"<b>広告名：</b>{r['AdName']}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{safe_div(clk,imp)*100:,.2f}%" if imp else "<b>CTR：</b>-",
        f"<b>CV数：</b>{int(cv) if cv else 'なし'}",
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
