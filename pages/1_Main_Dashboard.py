# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ------------------------------------------------------------
# 0. 画面全体の設定 & カード用 CSS
# ------------------------------------------------------------
st.set_page_config(page_title="メインダッシュボード", layout="wide")
st.markdown(
    """
    <style>
      .ad-card{
        padding:12px;border:1px solid #e6e6e6;border-radius:12px;
        background:#fafafa;height:100%;
      }
      .ad-card img{
        width:100%;height:180px;object-fit:cover;border-radius:8px;
      }
      .ad-caption{
        margin-top:8px;font-size:14px;line-height:1.6;text-align:left;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📊 Final_Ad_Data Dashboard")

# ------------------------------------------------------------
# 1. 認証 & データ取得
# ------------------------------------------------------------
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

query = """
SELECT *
FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
"""
st.write("🔄 データを取得中...")
try:
    df = client.query(query).to_dataframe()

    if df.empty:
        st.warning("⚠️ データがありません")
        st.stop()

    # ---------- 前処理 ----------
    if "カテゴリ" in df.columns:
        df["カテゴリ"] = (
            df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")
        )
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # ---------- 日付フィルタ ----------
    if "Date" in df.columns and not df["Date"].isnull().all():
        min_d, max_d = df["Date"].min().date(), df["Date"].max().date()
        sel = st.sidebar.date_input("日付フィルター", (min_d, max_d), min_d, max_d)
        if isinstance(sel, (list, tuple)) and len(sel) == 2:
            s, e = map(pd.to_datetime, sel)
            df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

    st.sidebar.header("🔍 フィルター")

    # ---------- クライアント検索 ----------
    clients = sorted(df["PromotionName"].dropna().unique())

    def _update():
        cs = st.session_state.client_search
        if cs in clients:
            st.session_state.selected_client = cs

    c_search = st.sidebar.text_input(
        "クライアント検索", "", placeholder="Enter で決定", key="client_search", on_change=_update
    )
    f_clients = [c for c in clients if c_search.lower() in c.lower()] if c_search else clients
    c_opts = ["すべて"] + f_clients
    sel_client = st.sidebar.selectbox(
        "クライアント", c_opts, index=c_opts.index(st.session_state.get("selected_client", "すべて"))
    )
    if sel_client != "すべて":
        df = df[df["PromotionName"] == sel_client]

    sel_cat = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(df["カテゴリ"].dropna().unique()))
    if sel_cat != "すべて":
        df = df[df["カテゴリ"] == sel_cat]

    sel_cmp = st.sidebar.selectbox(
        "キャンペーン名", ["すべて"] + sorted(df["CampaignName"].dropna().unique())
    )
    if sel_cmp != "すべて":
        df = df[df["CampaignName"] == sel_cmp]

    # ---------- 表形式 ----------
    st.subheader("📋 表形式データ")
    st.dataframe(df)

    # ---------- 欠損カラム補完 (1〜60 列) ----------
    for i in range(1, 61):
        col = str(i)
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    # ------------------------------------------------------------
    # 2. 配信バナー
    # ------------------------------------------------------------
    st.subheader("🖼️ 配信バナー")
    if "CloudStorageUrl" not in df.columns:
        st.info("CloudStorageUrl 列がありません")
        st.stop()

    st.write("🌟 CloudStorageUrl から画像を取得中...")

    img_df = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
    img_df["AdName"] = img_df["AdName"].astype(str).str.strip()
    img_df["CampaignId"] = img_df["CampaignId"].astype(str).str.strip()
    img_df["CloudStorageUrl"] = img_df["CloudStorageUrl"].astype(str).str.strip()
    img_df["AdNum"] = pd.to_numeric(img_df["AdName"], errors="coerce")
    img_df = img_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

    # ---------- CV件数列を必ず生成 ----------
    def _get_cv(r):
        n = r["AdNum"]
        if pd.isna(n):
            return 0
        col = str(int(n))
        return r[col] if col in r and isinstance(r[col], (int, float)) else 0

    img_df["CV件数"] = img_df.apply(_get_cv, axis=1).fillna(0).astype(int)
    if "CV件数" not in img_df.columns:  # 念のため
        img_df["CV件数"] = 0

    # ---------- 最新テキスト ----------
    latest = (
        img_df.sort_values("Date")
        .dropna(subset=["Date"])
        .loc[lambda d: d.groupby("AdName")["Date"].idxmax()]
    )
    latest_text = latest.set_index("AdName")["Description1ByAdType"].to_dict()

    # ---------- 集計 ----------
    agg = df.copy()
    agg["AdName"] = agg["AdName"].astype(str).str.strip()
    agg["CampaignId"] = agg["CampaignId"].astype(str).str.strip()
    agg["AdNum"] = pd.to_numeric(agg["AdName"], errors="coerce")
    agg = agg[agg["AdNum"].notna()]
    agg["AdNum"] = agg["AdNum"].astype(int)

    cv_sum = img_df.groupby(["CampaignId", "AdName"])["CV件数"].sum().reset_index()
    cap_df = (
        agg.groupby(["CampaignId", "AdName"])
        .agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"})
        .reset_index()
        .merge(cv_sum, on=["CampaignId", "AdName"], how="left")
    )
    cap_df["CTR"] = cap_df["Clicks"] / cap_df["Impressions"]
    cap_df["CPA"] = cap_df.apply(
        lambda r: (r["Cost"] / r["CV件数"]) if r["CV件数"] > 0 else pd.NA, axis=1
    )
    cap_map = cap_df.set_index(["CampaignId", "AdName"]).to_dict("index")

    img_df = img_df.merge(
        cap_df[["CampaignId", "AdName", "Cost", "Impressions", "Clicks", "CV件数", "CTR", "CPA"]],
        on=["CampaignId", "AdName"],
        how="left",
    )

    # ---------- 並び替え ----------
    sort_opt = st.radio("並び替え基準", ["AdNum", "CV件数(多)", "CPA(小)"])
    if sort_opt == "CV件数(多)":
        img_df = img_df[img_df["CV件数"] > 0].sort_values("CV件数", ascending=False)
    elif sort_opt == "CPA(小)":
        img_df = img_df[img_df["CPA"].notna()].sort_values("CPA")
    else:
        img_df = img_df.sort_values("AdNum")

    if img_df.empty:
        st.warning("⚠️ 表示できる画像がありません")
        st.stop()

    # ---------- 安全フォーマッタ ----------
    def fmt(val, pattern="{:,.0f}", alt="-"):
        return alt if val is None or pd.isna(val) else pattern.format(val)

    # ---------- カード描画 ----------
    cols = st.columns(5, gap="small")
    for i, (_, r) in enumerate(img_df.iterrows()):
        stat = cap_map.get((r["CampaignId"], r["AdName"]), {})
        cost = fmt(stat.get("Cost", 0)) + "円"
        imp = fmt(stat.get("Impressions", 0))
        clk = fmt(stat.get("Clicks", 0))
        ctr = fmt(stat.get("CTR") * 100 if pd.notna(stat.get("CTR")) else pd.NA, "{:.2f}%")
        cvn = int(stat.get("CV件数", 0))
        cvt = str(cvn) if cvn > 0 else "なし"
        cpa = fmt(stat.get("CPA"), "{:,.0f}円")

        cap_html = f"""
          <div class='ad-caption'>
            <b>広告名：</b>{r['AdName']}<br>
            <b>消化金額：</b>{cost}<br>
            <b>IMP：</b>{imp}<br>
            <b>クリック：</b>{clk}<br>
            <b>CTR：</b>{ctr}<br>
            <b>CV数：</b>{cvt}<br>
            <b>CPA：</b>{cpa}<br>
            <b>メインテキスト：</b>{latest_text.get(r['AdName'],'')}
          </div>
        """

        card = f"""
          <div class='ad-card'>
            <img src="{r['CloudStorageUrl']}">
            {cap_html}
          </div>
        """

        with cols[i % 5]:
            st.markdown(card, unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
