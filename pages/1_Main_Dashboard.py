import streamlit as st
from google.cloud import bigquery
import pandas as pd
import re

# ------------------------------------------------------------
# 0. ページ設定 & カード用 CSS
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 1. 認証 & データ取得
# ------------------------------------------------------------
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
with st.spinner("🔄 データを取得中..."):
    df = client.query(query).to_dataframe()

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
    min_d, max_d = df["Date"].min().date(), df["Date"].max().date()
    sel_date = st.sidebar.date_input("日付フィルター", (min_d, max_d),
                                    min_value=min_d, max_value=max_d)
    if isinstance(sel_date, (list, tuple)) and len(sel_date) == 2:
        s, e = map(pd.to_datetime, sel_date)
        df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]
    else:
        d = pd.to_datetime(sel_date).date()
        df = df[df["Date"].dt.date == d]

# ---------- サイドバー各種フィルタ ----------
st.sidebar.header("🔍 フィルター")
all_clients = sorted(df["PromotionName"].dropna().unique())

def update_client():
    cs = st.session_state.client_search
    if cs in all_clients:
        st.session_state.selected_client = cs

st.sidebar.text_input("クライアント検索", "", key="client_search",
                     placeholder="Enter で決定", on_change=update_client)

search_val = st.session_state.get("client_search", "")
filtered_clients = [c for c in all_clients if search_val.lower() in c.lower()] \
                    if search_val else all_clients
c_opts = ["すべて"] + filtered_clients
sel_client = st.sidebar.selectbox(
    "クライアント", c_opts,
    index=c_opts.index(st.session_state.get("selected_client", "すべて"))
)
if sel_client != "すべて":
    df = df[df["PromotionName"] == sel_client]

sel_cat = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(df["カテゴリ"].dropna().unique()))
if sel_cat != "すべて":
    df = df[df["カテゴリ"] == sel_cat]

sel_cmp = st.sidebar.selectbox("キャンペーン名", ["すべて"] + sorted(df["CampaignName"].dropna().unique()))
if sel_cmp != "すべて":
    df = df[df["CampaignName"] == sel_cmp]

# ---------- 1〜60 列補完 ----------
for i in range(1, 61):
    col = str(i)
    df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

# ------------------------------------------------------------
# 2. 画像バナー表示 (新しいコード)
# ------------------------------------------------------------
def calculate_and_display_banners(df):
    """
    フィルタリングされた DataFrame から画像バナー情報を計算し、表示する関数。
    """
    img_df = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
    if img_df.empty:
        st.warning("⚠️ 表示できる画像がありません")
        return

    img_df["AdNum"] = pd.to_numeric(img_df["AdName"], errors="coerce").fillna(-1).astype(int)

    conversion_cols = [str(i) for i in range(1, 61) if str(i) in df.columns]

    # キャンペーンIDごとのコンバージョン数を集計
    campaign_conversions = df.groupby("CampaignId")[conversion_cols].apply(
        lambda x: x.apply(lambda y: pd.to_numeric(y, errors='coerce')).fillna(0).sum(axis=1)
    ).sum(axis=1).rename("TotalConversions").fillna(0)

    # キャンペーンIDごとのコストと予算を取得
    campaign_cost = df.groupby("CampaignId")["Cost"].sum().rename("TotalCost").fillna(0)
    campaign_budget = df.groupby("CampaignId")["予算"].sum().rename("TotalBudget").fillna(0)

    img_df = pd.merge(img_df, campaign_conversions, on="CampaignId", how="left").fillna(0)
    img_df = pd.merge(img_df, campaign_cost, on="CampaignId", how="left").fillna(0)
    img_df = pd.merge(img_df, campaign_budget, on="CampaignId", how="left").fillna(0)

    img_df["CTR"] = (img_df["Clicks"].fillna(0) / img_df["Impressions"].fillna(1)) * 100
    img_df["CPA_Cost"] = img_df.apply(
        lambda row: row["TotalCost"] / row["TotalConversions"] if row["TotalConversions"] > 0 else float('nan'), axis=1
    )
    img_df["CPA_Budget"] = img_df.apply(
        lambda row: row["TotalBudget"] / row["TotalConversions"] if row["TotalConversions"] > 0 else float('nan'), axis=1
    )

    sort_opt = st.radio("並び替え基準", ["広告番号順", "コンバージョン数の多い順", "CPA(消化金額)の低い順"])
    if sort_opt == "コンバージョン数の多い順":
        img_df = img_df.sort_values("TotalConversions", ascending=False)
    elif sort_opt == "CPA(消化金額)の低い順":
        img_df = img_df.sort_values("CPA_Cost")
    else:
        img_df = img_df.sort_values("AdNum")

    cols = st.columns(5, gap="small")
    for idx, row in img_df.iterrows():
        ad_name = row["AdName"]
        cost = row["Cost"]
        impressions = row["Impressions"]
        clicks = row["Clicks"]
        ctr = row["CTR"]
        total_conversions = int(row["TotalConversions"])
        cpa_cost = row["CPA_Cost"]
        cpa_budget = row["CPA_Budget"]
        description = row.get("Description1ByAdType", "")
        canva_url = row.get("canvaURL", "")

        canva_html = ""
        if pd.notna(canva_url) and canva_url.startswith("http"):
            canva_links = [link.strip() for link in re.split(r'[,\s]+', str(canva_url))]
            canva_html = ", ".join(
                f'<a href="{link}" target="_blank" rel="noopener">canvaURL{i+1 if len(canva_links)>1 else ""}↗️</a>'
                for i, link in enumerate(canva_links)
            )
        else:
            canva_html = '<span class="gray-text">canvaURL：なし✖</span>'

        cap_html = f"""
            <div class='banner-caption'>
                <b>広告名：</b>{ad_name}<br>
                <b>消化金額：</b>{cost:,.0f}円<br>
                <b>IMP：</b>{impressions:,.0f}<br>
                <b>クリック：</b>{clicks:,.0f}<br>
                <b>CTR：</b>{ctr:.2f}%<br>
                <b>CV数：</b>{total_conversions if total_conversions > 0 else 'なし'}<br>
                <b>CPA(消化金額)：</b>{f'{cpa_cost:.0f}円' if pd.notna(cpa_cost) else '-'}
                <b>CPA(予算)：</b>{f'{cpa_budget:.0f}円' if pd.notna(cpa_budget) else '-'}
                {canva_html}<br>
                <b>メインテキスト：</b>{description}
            </div>
        """

        card_html = f"""
            <div class='banner-card'>
                <a href="{row['CloudStorageUrl']}" target="_blank" rel="noopener">
                    <img src="{row['CloudStorageUrl']}">
                </a>
                {cap_html}
            </div>
        """

        with cols[idx % 5]:
            st.markdown(card_html, unsafe_allow_html=True)

# メインの処理
st.subheader("🌟 並び替え") # 並び替えのラジオボタンを関数の外に出す
sort_opt = st.radio("並び替え基準", ["広告番号順", "コンバージョン数の多い順", "CPA(消化金額)の低い順"])

filtered_df = df.copy() # フィルタリングされた DataFrame を作成

if sel_client != "すべて":
    filtered_df = filtered_df[filtered_df["PromotionName"] == sel_client]
if sel_cat != "すべて":
    filtered_df = filtered_df[filtered_df["カテゴリ"] == sel_cat]
if sel_cmp != "すべて":
    filtered_df = filtered_df[filtered_df["CampaignName"] == sel_cmp]
if not df["Date"].isnull().all():
    if isinstance(sel_date, (list, tuple)) and len(sel_date) == 2:
        s, e = map(pd.to_datetime, sel_date)
        filtered_df = filtered_df[(filtered_df["Date"].dt.date >= s.date()) & (filtered_df["Date"].dt.date <= e.date())]
    else:
        d = pd.to_datetime(sel_date).date()
        filtered_df = filtered_df[filtered_df["Date"].dt.date == d]

calculate_and_display_banners(filtered_df)

# デバッグ出力は削除しました
