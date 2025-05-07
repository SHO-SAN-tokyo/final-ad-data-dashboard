import streamlit as st
from google.cloud import bigquery
import pandas as pd, numpy as np, re

# --- ページ設定 & CSS ---
st.set_page_config(page_title="Ad_Drive", layout="wide")
st.title("🫧 Ad Drive")

st.markdown("""
    <style>
      .banner-card{padding:12px 12px 20px;border:1px solid #e6e6e6;border-radius:12px;
                   background:#fafafa;height:100%;margin-bottom:14px;}
      .banner-card img{width:100%;height:203px;object-fit:cover;border-radius:8px;cursor:pointer;}
      .banner-caption{margin-top:8px;font-size:14px;line-height:1.6;text-align:left;}
      .gray-text{color:#888;}
    </style>
    """, unsafe_allow_html=True)

# --- BigQuery認証 & パラメータ取得 ---
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

query_params = st.query_params
preselected_client_id = query_params.get("client_id", None)

# --- データ取得 ---
query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
msg_slot = st.empty()

# データ取得中に表示（緑枠のようなHTML）
msg_slot.markdown("""
    <div style='background-color:#e6f4ea;padding:10px 20px;border-radius:8px;color:#10733f;
                border:1px solid #b2e2c4;font-size:16px;margin-bottom:10px;'>
        🔄️ データ取得中...
    </div>
""", unsafe_allow_html=True)

# データ取得処理
query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
df = bq.query(query).to_dataframe()

# データ取得後にメッセージを変更
msg_slot.markdown("""
    <div style='background-color:#e6f4ea;padding:10px 20px;border-radius:8px;color:#10733f;
                border:1px solid #b2e2c4;font-size:16px;margin-bottom:10px;'>
        ✅ データ取得完了
    </div>
""", unsafe_allow_html=True)
if df.empty:
    st.warning("⚠️ データがありません"); st.stop()

# ClientSettingsも取得
client_settings_query = "SELECT client_id, client_name FROM careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings"
client_settings_df = bq.query(client_settings_query).to_dataframe()
client_name_map = dict(zip(client_settings_df["client_id"], client_settings_df["client_name"]))

# --- パラメータによるフィルター適用 ---
if preselected_client_id and preselected_client_id in client_name_map:
    preselected_client_name = client_name_map[preselected_client_id]
    st.markdown(f"<h2 style='color:#000;margin-top:1rem;'>🔶 {preselected_client_name}</h2>", unsafe_allow_html=True)
    df = df[df["client_name"] == preselected_client_name]

# --- 前処理 ---
df["カテゴリ"] = df.get("カテゴリ", "").astype(str).str.strip().replace("", "未設定").fillna("未設定")
df["Date"] = pd.to_datetime(df.get("Date"), errors="coerce")

# --- セッションステートの初期化 ---
for key in ["select_all_clients", "select_all_categories", "select_all_campaigns"]:
    if key not in st.session_state:
        st.session_state[key] = False

# --- ドリルダウン対応マルチセレクトフィルター ---
# --- ドリルダウン対応マルチセレクトフィルター ---
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
col1, col2, col3, col4 = st.columns([1, 2, 2, 3])  # 横幅比率を調整してバランスを整える

with col1:
    sel_date = st.date_input("🔍日付フィルター", (dmin, dmax), min_value=dmin, max_value=dmax)

with col2:
    client_all = sorted(df["PromotionName"].dropna().unique())
    with st.container():
        st.checkbox("✅ クライアントをすべて選択", key="select_all_clients", value=False)
        sel_client = st.multiselect(
            "クライアント",
            options=client_all,
            default=client_all if st.session_state.select_all_clients else [],
            key="client_selector"
        )

df_client = df[df["PromotionName"].isin(sel_client)] if sel_client else df.copy()

with col3:
    cat_all = sorted(df_client["カテゴリ"].dropna().unique())
    with st.container():
        st.checkbox("✅ カテゴリをすべて選択", key="select_all_categories", value=False)
        sel_cat = st.multiselect(
            "カテゴリ",
            options=cat_all,
            default=cat_all if st.session_state.select_all_categories else [],
            key="cat_selector"
        )

df_cat = df_client[df_client["カテゴリ"].isin(sel_cat)] if sel_cat else df_client.copy()

with col4:
    camp_all = sorted(df_cat["CampaignName"].dropna().unique())
    with st.container():
        st.checkbox("✅ キャンペーン名をすべて選択", key="select_all_campaigns", value=False)
        sel_campaign = st.multiselect(
            "キャンペーン名",
            options=camp_all,
            default=camp_all if st.session_state.select_all_campaigns else [],
            key="camp_selector"
        )



# --- フィルター適用 ---
if isinstance(sel_date, (list, tuple)) and len(sel_date) == 2:
    s, e = map(pd.to_datetime, sel_date)
    df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]
else:
    d = pd.to_datetime(sel_date).date()
    df = df[df["Date"].dt.date == d]

if sel_client:
    df = df[df["PromotionName"].isin(sel_client)]
if sel_cat:
    df = df[df["カテゴリ"].isin(sel_cat)]
if sel_campaign:
    df = df[df["CampaignName"].isin(sel_campaign)]


for i in range(1, 61):
    c = str(i)
    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

for c in ["Cost", "Impressions", "Clicks", "コンバージョン数", "Reach"]:
    df[c] = pd.to_numeric(df.get(c), errors="coerce")

# --- サマリー表 ---
tot_cost = df["Cost"].sum()
tot_imp  = df["Impressions"].sum()

latest_idx = (df.dropna(subset=["Date"]).sort_values("Date").groupby("CampaignId")["Date"].idxmax())
latest_df = df.loc[latest_idx].copy()

tot_conv = latest_df["コンバージョン数"].fillna(0).sum()
tot_clk_all = df["Clicks"].sum()

div = lambda n, d: np.nan if (d == 0 or pd.isna(d)) else n / d
disp = lambda v, u="": "-" if pd.isna(v) else f"{int(round(v)):,}{u}"
disp_percent = lambda v: "-" if pd.isna(v) else f"{v:.2f}%"

summary = pd.DataFrame({
    "指標": ["CPA", "コンバージョン数", "CVR", "消化金額",
           "インプレッション", "CTR", "CPC", "クリック数", "CPM"],
    "値": [
        disp(div(tot_cost, tot_conv), "円"),
        disp(tot_conv),
        disp_percent(div(tot_conv, tot_clk_all) * 100),
        disp(tot_cost, "円"),
        disp(tot_imp),
        disp_percent(div(tot_clk_all, tot_imp) * 100),
        disp(div(tot_cost, tot_clk_all), "円"),
        disp(tot_clk_all),
        disp(div(tot_cost * 1000, tot_imp), "円"),
    ]
})

# --- 選択中フィルターの表示（キャンペーン名含む） ---
selected_filters = []

# 日付範囲（文字列で整形）
if isinstance(sel_date, (list, tuple)) and len(sel_date) == 2:
    date_range = f"{sel_date[0]} 〜 {sel_date[1]}"
else:
    date_range = str(sel_date)
selected_filters.append(f"<b>📅 日付：</b>{date_range}")

selected_filters.append(f"<b>👤 クライアント：</b>{sel_client}")
selected_filters.append(f"<b>🗂️ カテゴリ：</b>{sel_cat}")
selected_filters.append(f"<b>📢 キャンペーン名：</b>{sel_campaign}")

# 表示する
st.markdown(
    "<div style='margin-bottom: 1rem; font-size: 16px; color: #555;'>" +
    "｜".join(selected_filters) +
    "</div>",
    unsafe_allow_html=True
)


st.subheader("💠広告数値")
# --- カード表示レイアウトに変更 ---
st.markdown("""
<style>
.metric-card {
    background-color: #2c2f36;
    color: #fcefc7;
    padding: 20px 25px;
    border-radius: 12px;
    text-align: center;
    font-size: 16px;
    margin-bottom: 10px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.25);
}
.metric-big {
    font-size: 40px;
    font-weight: bold;
    margin-top: 10px;
}
.metric-small {
    font-size: 28px;
    font-weight: bold;
    margin-top: 8px;
}
</style>
""", unsafe_allow_html=True)

cpa = disp(div(tot_cost, tot_conv), "円")
cv = disp(tot_conv)
cvr = disp_percent(div(tot_conv, tot_clk_all) * 100)
cost = disp(tot_cost, "円")
imp = disp(tot_imp)
ctr = disp_percent(div(tot_clk_all, tot_imp) * 100)
cpc = disp(div(tot_cost, tot_clk_all), "円")
clk = disp(tot_clk_all)
cpm = disp(div(tot_cost * 1000, tot_imp), "円")

# 1段目（大きなカード）
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(f"<div class='metric-card'>CPA - 獲得単価<div class='metric-big'>{cpa}</div></div>", unsafe_allow_html=True)
with c2:
    st.markdown(f"<div class='metric-card'>コンバージョン数<div class='metric-big'>{cv}</div></div>", unsafe_allow_html=True)
with c3:
    st.markdown(f"<div class='metric-card'>CVR - コンバージョン率<div class='metric-big'>{cvr}</div></div>", unsafe_allow_html=True)

# 2段目（小さめのカード）
c4, c5, c6, c7, c8 = st.columns(5)
with c4:
    st.markdown(f"<div class='metric-card'>消化金額<div class='metric-small'>{cost}</div></div>", unsafe_allow_html=True)
with c5:
    st.markdown(f"<div class='metric-card'>インプレッション<div class='metric-small'>{imp}</div></div>", unsafe_allow_html=True)
with c6:
    st.markdown(f"<div class='metric-card'>CTR - クリック率<div class='metric-small'>{ctr}</div></div>", unsafe_allow_html=True)
with c7:
    st.markdown(f"<div class='metric-card'>CPM<div class='metric-small'>{cpm}</div></div>", unsafe_allow_html=True)
with c8:
    st.markdown(f"<div class='metric-card'>クリック<div class='metric-small'>{clk}</div></div>", unsafe_allow_html=True)


# --- バナー表示（いつもの方式） ---
img = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
img["AdName"] = img["AdName"].astype(str).str.strip()
img["CampaignId"] = img["CampaignId"].astype(str).str.strip()
img["AdNum"] = pd.to_numeric(img["AdName"], errors="coerce")

latest = (img.dropna(subset=["Date"])
          .sort_values("Date")
          .loc[lambda d: d.groupby(["CampaignId", "AdName"])["Date"].idxmax()]
          .copy())

def row_cv(r):
    n = r["AdNum"]
    col = str(int(n)) if pd.notna(n) else None
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

st.markdown("<div style='margin-top:3.5rem;'></div>", unsafe_allow_html=True)
st.subheader("💠配信バナー")

opt = st.radio("並び替え基準",
               ["広告番号順", "コンバージョン数の多い順", "CPAの低い順"])
if opt == "コンバージョン数の多い順":
    latest = latest[latest["CV件数"] > 0].sort_values("CV件数", ascending=False)
elif opt == "CPAの低い順":
    latest = latest[latest["CPA_sort"].notna()].sort_values("CPA_sort")
else:
    latest = latest.sort_values("AdNum")

def urls(raw): return [u for u in re.split(r"[,\\s]+", str(raw or "")) if u.startswith("http")]

cols = st.columns(5, gap="small")
for i, (_, r) in enumerate(latest.iterrows()):
    key = (r["CampaignId"], r["AdName"])
    s = sum_map.get(key, {})
    cost = s.get("Cost", 0)
    imp = s.get("Impressions", 0)
    clk = s.get("Clicks", 0)
    cv = int(r["CV件数"])
    cpa = div(cost, cv)
    ctr = div(clk, imp)
    text = r.get("Description1ByAdType", "")

    lnks = urls(r.get("canvaURL", ""))
    canva_html = (" ,".join(
                    f'<a href="{u}" target="_blank">canvaURL{i+1 if len(lnks)>1 else ""}↗️</a>'
                    for i, u in enumerate(lnks))
                  if lnks else '<span class="gray-text">canvaURL：なし✖</span>')

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
