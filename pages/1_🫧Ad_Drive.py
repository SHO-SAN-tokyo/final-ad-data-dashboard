# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd, numpy as np, re

# ---------- 0. ページ設定 & CSS ----------
st.set_page_config(page_title="Ad_Drive", layout="wide")
st.title("🫧 Ad Drive すべての広告")

st.markdown("""
<style>
 .banner-card{padding:12px 12px 20px;border:1px solid #e6e6e6;border-radius:12px;
              background:#fafafa;height:100%;margin-bottom:14px;}
 .banner-card img{width:100%;height:180px;object-fit:cover;border-radius:8px;cursor:pointer;}
 .banner-caption{margin-top:8px;font-size:14px;line-height:1.6;text-align:left;}
 .gray-text{color:#888;}
</style>
""", unsafe_allow_html=True)

# ---------- 1. BigQuery ----------
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
with st.spinner("🔄 データを取得中..."):
    df = bq.query(query).to_dataframe()
if df.empty:
    st.warning("⚠️ データがありません"); st.stop()

# ---------- 2. 前処理 ----------
df["カテゴリ"] = df.get("カテゴリ","").astype(str).str.strip().replace("", "未設定").fillna("未設定")
df["Date"]     = pd.to_datetime(df.get("Date"), errors="coerce")

dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
sel = st.sidebar.date_input("日付フィルター", (dmin, dmax), min_value=dmin, max_value=dmax)
if isinstance(sel, (list, tuple)) and len(sel) == 2:
    s, e = map(pd.to_datetime, sel)
    df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]
else:
    d = pd.to_datetime(sel).date()
    df = df[df["Date"].dt.date == d]

st.sidebar.header("🔍 フィルター")
for col, lbl in [("PromotionName", "クライアント"),
                 ("カテゴリ", "カテゴリ"),
                 ("CampaignName", "キャンペーン名")]:
    opts = ["すべて"] + sorted(df[col].dropna().unique())
    sel  = st.sidebar.selectbox(lbl, opts)
    if sel != "すべて":
        df = df[df[col] == sel]

# 1〜60 列数値化
for i in range(1, 61):
    c = str(i)
    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

# ---------- 3. サマリー表 ----------
for c in ["Cost", "Impressions", "Clicks", "コンバージョン数", "Reach"]:
    df[c] = pd.to_numeric(df.get(c), errors="coerce")

tot_cost = df["Cost"].sum()
tot_imp  = df["Impressions"].sum()
tot_clk  = df["Clicks"].sum()

# ★★ 修正版 → コンバージョン数は「最新行」だけを合計 ★★
conv_latest_idx = (df.dropna(subset=["Date"])
                    .sort_values("Date")
                    .groupby(["CampaignId", "AdName"])["Date"].idxmax())
tot_conv = df.loc[conv_latest_idx, "コンバージョン数"].fillna(0).sum()

tot_reach = df["Reach"].sum()

div  = lambda n, d: np.nan if (d == 0 or pd.isna(d)) else n / d
disp = lambda v, u="": "-" if pd.isna(v) else f"{int(round(v)):,}{u}"

summary = pd.DataFrame({
    "指標": ["CPA", "コンバージョン数", "CVR", "消化金額",
           "インプレッション", "CTR", "CPC", "クリック数",
           "CPM", "フリークエンシー"],
    "値": [
        disp(div(tot_cost, tot_conv), "円"),
        disp(tot_conv),
        disp(div(tot_conv, tot_clk) * 100, "%"),
        disp(tot_cost, "円"),
        disp(tot_imp),
        disp(div(tot_clk, tot_imp) * 100, "%"),
        disp(div(tot_cost, tot_clk), "円"),
        disp(tot_clk),
        disp(div(tot_cost * 1000, tot_imp), "円"),
        disp(div(tot_imp, tot_reach))
    ]
})
st.subheader("💠広告数値")
st.table(summary)

# ---------- 4. 画像バナー（ロジックは以前と同じ） ----------
img = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
img["AdName"]     = img["AdName"].astype(str).str.strip()
img["CampaignId"] = img["CampaignId"].astype(str).str.strip()
img["AdNum"]      = pd.to_numeric(img["AdName"], errors="coerce")

latest = (img.dropna(subset=["Date"])
            .sort_values("Date")
            .loc[lambda d: d.groupby(["CampaignId", "AdName"])["Date"].idxmax()]
            .copy())

def row_cv(r):
    n = r["AdNum"]; col = str(int(n)) if pd.notna(n) else None
    return r[col] if col and col in r and isinstance(r[col], (int, float)) else 0
latest["CV件数"] = latest.apply(row_cv, axis=1)

agg = (df.assign(AdName=lambda d: d["AdName"].astype(str).str.strip(),
                 CampaignId=lambda d: d["CampaignId"].astype(str).str.strip())
          .groupby(["CampaignId", "AdName"])
          .agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"})
          .reset_index())

latest = latest.merge(
    agg[["CampaignId", "AdName", "Cost"]],
    on=["CampaignId", "AdName"],
    how="left",
    suffixes=("", "_agg")
)
latest["CPA_sort"] = latest.apply(lambda r: div(r["Cost_agg"], r["CV件数"]), axis=1)
sum_map = agg.set_index(["CampaignId", "AdName"]).to_dict("index")

st.subheader("💠配信バナー")
opt = st.radio("並び替え基準",
               ["広告番号順", "コンバージョン数の多い順", "CPAの低い順"])
if opt == "コンバージョン数の多い順":
    latest = latest[latest["CV件数"] > 0].sort_values("CV件数", ascending=False)
elif opt == "CPAの低い順":
    latest = latest[latest["CPA_sort"].notna()].sort_values("CPA_sort")
else:
    latest = latest.sort_values("AdNum")

def urls(raw):
    return [u for u in re.split(r"[,\s]+", str(raw or "")) if u.startswith("http")]

cols = st.columns(5, gap="small")
for i, (_, r) in enumerate(latest.iterrows()):
    key = (r["CampaignId"], r["AdName"])
    s   = sum_map.get(key, {})
    cost, imp, clk = s.get("Cost", 0), s.get("Impressions", 0), s.get("Clicks", 0)
    cv   = int(r["CV件数"])
    cpa  = div(cost, cv)
    ctr  = div(clk, imp)
    text = r.get("Description1ByAdType", "")

    lnk = urls(r.get("canvaURL", ""))
    canva_html = (" ,".join(
                    f'<a href="{u}" target="_blank">canvaURL{i+1 if len(lnk)>1 else ""}↗️</a>'
                    for i, u in enumerate(lnk))
                 if lnk else '<span class="gray-text">canvaURL：なし✖</span>')

    caption = [
        f"<b>広告名：</b>{r['AdName']}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{int(round(ctr*100)):,}%" if pd.notna(ctr) else "<b>CTR：</b>-",
        f"<b>CV数：</b>{cv if cv else 'なし'}",
        f"<b>CPA：</b>{int(round(cpa)):,}円" if pd.notna(cpa) else "<b>CPA：</b>-",
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
