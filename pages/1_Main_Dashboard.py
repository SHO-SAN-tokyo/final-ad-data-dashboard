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
        if col in r:
            value = pd.to_numeric(r[col], errors='coerce')
            return value if pd.notna(value) and isinstance(value, (int, float)) else 0
        else:
            return 0

    img_df["CV件数_base"] = img_df.apply(get_cv, axis=1) # 元のCV件数を保持

    # デバッグ出力: AdName、AdNum、CV件数_base を確認
    st.subheader("デバッグ: img_df の AdName と AdNum と CV件数_base")
    st.dataframe(img_df[["AdName", "AdNum", "CV件数_base"]].head(10))

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
    agg_df["CampaignId"] = agg_df["CampaignId"].astype(str).str.strip()
    agg_df["AdNum"] = pd.to_numeric(agg_df["AdName"], errors="coerce")
    agg_df = agg_df[agg_df["AdNum"].notna()]
    agg_df["AdNum"] = agg_df["AdNum"].astype(int)

    cv_sum_df = img_df.groupby(["CampaignId", "AdName"])["CV件数_base"].sum().reset_index() # 元のCV件数を使用

    caption_df = (
        agg_df.groupby(["CampaignId", "AdName"])
        .agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"})
        .reset_index()
        .merge(cv_sum_df, on=["CampaignId", "AdName"], how="left")
    )
    caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
    caption_df["CPA"] = caption_df.apply(
        lambda r: (r["Cost"] / r["CV件数_base"]) if pd.notna(r["CV件数_base"]) and r["CV件数_base"] > 0 else pd.NA,
        axis=1,
    )

    # デバッグ: caption_df の内容をすべて表示
    st.subheader("デバッグ: caption_df の内容 (すべて)")
    st.dataframe(caption_df)

    # デバッグ: img_df の結合キーの値 (すべて)
    st.subheader("デバッグ: img_df の結合キーの値 (すべて)")
    st.dataframe(img_df[["CampaignId", "AdName"]])

    # デバッグ: caption_df の結合キーの値 (すべて)
    st.subheader("デバッグ: caption_df の結合キーの値 (すべて)")
    st.dataframe(caption_df[["CampaignId", "AdName"]])

    # マージ前に結合キーの空白文字を除去 (念のため再度)
    img_df["CampaignId"] = img_df["CampaignId"].str.strip()
    img_df["AdName"] = img_df["AdName"].str.strip()
    caption_df["CampaignId"] = caption_df["CampaignId"].str.strip()
    caption_df["AdName"] = caption_df["AdName"].str.strip()

    # CPA / CV件数 を付与
    merged_img_df = img_df.merge(
        caption_df[["CampaignId", "AdName", "CV件数_base", "CPA"]],
        on=["CampaignId", "AdName"],
        how="left"
    )

    st.subheader("デバッグ: マージ後 merged_img_df")
    st.write(f"merged_img_df の行数 (マージ後): {len(merged_img_df)}")
    st.dataframe(merged_img_df)

    # 正しい CV件数と CPA の列を選択し、分かりやすい名前に変更
    if "CV件数_base_x" in merged_img_df.columns:
        img_df["CV件数_計算"] = merged_img_df["CV件数_base_x"]
    elif "CV件数_base_y" in merged_img_df.columns:
        img_df["CV件数_計算"] = merged_img_df["CV件数_base_y"]
    else:
        img_df["CV件数_計算"] = pd.NA  # 該当する列がない場合の処理

    if "CPA_x" in merged_img_df.columns:
        img_df["CPA_計算"] = merged_img_df["CPA_x"]
    elif "CPA_y" in merged_img_df.columns:
        img_df["CPA_計算"] = merged_img_df["CPA_y"]
    else:
        img_df["CPA_計算"] = pd.NA  # 該当する列がない場合の処理

    # 新しい列名で更新 (元の列を上書きしない)
    img_df["CPA_表示用"] = pd.to_numeric(img_df["CPA_計算"], errors="coerce")
    img_df["CV件数_表示用"] = pd.to_numeric(img_df["CV件数_計算"], errors="coerce").fillna(0)

    # 不要になった計算用列を削除 (任意)
    img_df = img_df.drop(columns=["CPA_計算", "CV件数_計算", "CV件数_base_x", "CV件数_base_y", "CPA_x", "CPA_y", "CV件数_base"], errors='ignore')
    img_df = img_df.rename(columns={'CPA_表示用': 'CPA', 'CV件数_表示用': 'CV件数'})


    caption_map = caption_df.set_index(["CampaignId", "AdName"]).to_dict("index")

    # デバッグコードを追加
    st.subheader("デバッグ: caption_map の最初のキー")
    st.write(list(caption_map.keys())[:5])

    def parse_canva_links(raw: str) -> list[str]:
        parts = re.split(r'[,\s]+', str(raw or ""))
        return [p for p in parts if p.startswith("http")]

    st.subheader("デバッグ表示ループ前の img_df (列名整理後)")
    st.dataframe(img_df)

    cols = st.columns(5, gap="small")

    for idx, (_, row) in enumerate(img_df.iterrows()):
        ad  = row["AdName"]
        cid = row["CampaignId"]
        st.write(f"デバッグ表示ループ: cid='{cid}', ad='{ad}'")
        v   = caption_map.get((cid, ad), {})
        cost, imp, clicks = v.get("Cost", 0), v.get("Impressions", 0), v.get("Clicks", 0)
        ctr, cpa_loop = v.get("CPA"), v.get("CTR")
        cv_loop = v.get("CV件数", 0)

        # メインテキストの取得
        text = latest_text_map.get(ad, "")

        # canvaURL
        links = parse_canva_links(row.get("canvaURL", ""))
        if links:
            canva_html = ", ".join(
                f'<a href="{l}" target="_blank" rel="noopener">canvaURL{i+1 if len(links)>1 else ""}↗️</a>'
                for i, l in enumerate(links)
            )
        else:
            canva_html = '<span class="gray-text">canvaURL：なし✖</span>'

        # ---------- caption 文字列 ----------
        cap_html = "<div class='banner-caption'>"
        cap_html += f"<b>広告名：</b>{ad}<br>"
        cap_html += f"<b>消化金額：</b>{cost:,.0f}円<br>"
        cap_html += f"<b>IMP：</b>{imp:,.0f}<br>"
        cap_html += f"<b>クリック：</b>{clicks:,.0f}<br>"
        if pd.notna(ctr):
            cap_html += f"<b>CTR：</b>{ctr*100:.2f}%<br>"
        else:
            cap_html += "<b>CTR：</b>-<br>"
        cap_html += f"<b>CV数：</b>{int(row['CV件数']) if pd.notna(row['CV件数']) and row['CV件数'] > 0 else 'なし'}<br>"
        cap_html += f"<b>CPA：</b>{row['CPA']:,.0f}円<br>" if pd.notna(row['CPA']) else "<b>CPA：</b>-<br>"
        cap_html += f"{canva_html}<br>"
        cap_html += f"<b>メインテキスト：</b>{text}</div>"

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
