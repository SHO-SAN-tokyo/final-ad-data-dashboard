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
    # 2. 画像バナー表示
    # ------------------------------------------------------------
    st.subheader("🌟 並び替え")
    img_df = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()

    st.subheader("デバッグ: img_df 初期化直後")
    st.write(f"img_df の行数: {len(img_df)}")
    st.dataframe(img_df)

    if img_df.empty:
        st.warning("⚠️ 表示できる画像がありません (CloudStorageUrl フィルタ後)")
        st.stop()

    img_df["AdName"]     = img_df["AdName"].astype(str).str.strip()
    img_df["CampaignId"]  = img_df["CampaignId"].astype(str).str.strip()
    img_df["CloudStorageUrl"] = img_df["CloudStorageUrl"].astype(str).str.strip()
    img_df["AdNum"] = pd.to_numeric(img_df["AdName"], errors="coerce")
    img_df = img_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

    st.subheader("デバッグ: 重複削除後 img_df")
    st.write(f"img_df の行数 (重複削除後): {len(img_df)}")
    st.dataframe(img_df)

    def get_cv(r):
        n = r["AdNum"]
        if pd.isna(n): return 0
        col = str(int(n))
        return r[col] if col in r and isinstance(r[col], (int, float)) else 0
    img_df["CV件数_base"] = img_df.apply(get_cv, axis=1) # 元のCV件数を保持

    latest_text_map = {}
    if "Date" in img_df.columns:
        latest = (img_df.sort_values("Date")
                    .dropna(subset=["Date"])
                    .loc[lambda d: d.groupby("AdName")["Date"].idxmax()])
        latest_text_map = latest.set_index("AdName")["Description1ByAdType"].to_dict()
    else:
        st.warning("⚠️ 'Date' 列が img_df に存在しないため、メインテキストの取得をスキップします。")

    agg_df = df.copy()
    agg_df["AdName"] = agg_df["AdName"].astype(str).str.strip()
    agg_
