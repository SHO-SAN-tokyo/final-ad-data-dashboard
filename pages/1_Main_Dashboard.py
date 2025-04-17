# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd

# ------------------------------------------------------------
# 0. 画面全体の設定 & カード用 CSS を注入  ★★ 追加部分 ★★
# ------------------------------------------------------------
st.set_page_config(page_title="メインダッシュボード", layout="wide")
st.markdown(
    """
    <style>
      /* 画像とキャプションを 1 つのカードにまとめる */
      .ad-card{
        padding:12px;
        border:1px solid #e6e6e6;
        border-radius:12px;
        background:#fafafa;
        height:100%;
      }
      .ad-card img{
        width:100%;
        height:180px;            /* 高さを固定して object‑fit でトリミング */
        object-fit:cover;
        border-radius:8px;
      }
      .ad-caption{
        margin-top:8px;
        font-size:14px;
        line-height:1.6;
        text-align:left;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📊 Final_Ad_Data Dashboard")

# ------------------------------------------------------------
# 1. 認証 & データ取得（ここは変更なし）
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
    else:
        st.success("✅ データ取得成功！")

        # ---------- 前処理（変更なし） ----------
        if "カテゴリ" in df.columns:
            df["カテゴリ"] = (
                df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")
            )
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # ---------- 日付フィルタ ----------
        if "Date" in df.columns and not df["Date"].isnull().all():
            min_date = df["Date"].min().date()
            max_date = df["Date"].max().date()
            selected_date = st.sidebar.date_input(
                "日付フィルター",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
            if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
                s, e = map(pd.to_datetime, selected_date)
                date_filtered_df = df[
                    (df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())
                ]
            else:
                d = pd.to_datetime(selected_date).date()
                date_filtered_df = df[df["Date"].dt.date == d]
        else:
            date_filtered_df = df.copy()

        st.sidebar.header("🔍 フィルター")

        # ---------- クライアント検索 ----------
        all_clients = sorted(date_filtered_df["PromotionName"].dropna().unique())

        def update_client():
            cs = st.session_state.client_search
            if cs in all_clients:
                st.session_state.selected_client = cs

        client_search = st.sidebar.text_input(
            "クライアント検索",
            "",
            placeholder="必ず正しく入力してEnterを押す",
            key="client_search",
            on_change=update_client,
        )
        filtered_clients = (
            [c for c in all_clients if client_search.lower() in c.lower()]
            if client_search
            else all_clients
        )

        client_options = ["すべて"] + filtered_clients
        default_idx = client_options.index(
            st.session_state.get("selected_client", "すべて")
        )
        selected_client = st.sidebar.selectbox("クライアント", client_options, index=default_idx)

        client_filtered_df = (
            date_filtered_df[date_filtered_df["PromotionName"] == selected_client]
            if selected_client != "すべて"
            else date_filtered_df.copy()
        )

        # ---------- カテゴリ & キャンペーン ----------
        sel_cat = st.sidebar.selectbox(
            "カテゴリ", ["すべて"] + sorted(client_filtered_df["カテゴリ"].dropna().unique())
        )
        cat_filtered_df = (
            client_filtered_df[client_filtered_df["カテゴリ"] == sel_cat]
            if sel_cat != "すべて"
            else client_filtered_df.copy()
        )

        sel_campaign = st.sidebar.selectbox(
            "キャンペーン名", ["すべて"] + sorted(cat_filtered_df["CampaignName"].dropna().unique())
        )
        filtered_df = (
            cat_filtered_df[cat_filtered_df["CampaignName"] == sel_campaign]
            if sel_campaign != "すべて"
            else cat_filtered_df.copy()
        )

        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        # ---------- 欠損カラム補完 ----------
        for i in range(1, 61):
            col = str(i)
            filtered_df[col] = pd.to_numeric(filtered_df.get(col, 0), errors="coerce").fillna(0)

        # ------------------------------------------------------------
        # 2. 配信バナー表示ブロック（カードレイアウトに変更） ★★ ここを調整 ★★
        # ------------------------------------------------------------
        st.subheader("🖼️ 配信バナー")
        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🌟 CloudStorageUrl から画像を取得中...")

            image_df = filtered_df[
                filtered_df["CloudStorageUrl"].astype(str).str.startswith("http")
            ].copy()

            image_df["AdName"] = image_df["AdName"].astype(str).str.strip()
            image_df["CampaignId"] = image_df["CampaignId"].astype(str).str.strip()
            image_df["CloudStorageUrl"] = image_df["CloudStorageUrl"].astype(str).str.strip()
            image_df["AdNum"] = pd.to_numeric(image_df["AdName"], errors="coerce")
            image_df = image_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            # Cost / Impression / Clicks 数値化
            for col in ["Cost", "Impressions", "Clicks"]:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")

            # CV 件数算出
            def get_cv(row):
                adnum = row["AdNum"]
                if pd.isna(adnum):
                    return 0
                c = str(int(adnum))
                return row[c] if c in row and isinstance(row[c], (int, float)) else 0

            image_df["CV件数"] = image_df.apply(get_cv, axis=1)

            # 最新テキスト
            latest_rows = (
                image_df.sort_values("Date")
                .dropna(subset=["Date"])
                .loc[lambda x: x.groupby("AdName")["Date"].idxmax()]
            )
            latest_text_map = latest_rows.set_index("AdName")["Description1ByAdType"].to_dict()

            # 集計 (Cost/Imp/Clicks)
            agg_df = filtered_df.copy()
            agg_df["AdName"] = agg_df["AdName"].astype(str).str.strip()
            agg_df["CampaignId"] = agg_df["CampaignId"].astype(str).str.strip()
            agg_df["AdNum"] = pd.to_numeric(agg_df["AdName"], errors="coerce")
            agg_df = agg_df[agg_df["AdNum"].notna()]
            agg_df["AdNum"] = agg_df["AdNum"].astype(int)

            cv_sum_df = image_df.groupby(["CampaignId", "AdName"])["CV件数"].sum().reset_index()

            caption_df = (
                agg_df.groupby(["CampaignId", "AdName"])
                .agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"})
                .reset_index()
                .merge(cv_sum_df, on=["CampaignId", "AdName"], how="left")
            )
            caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
            caption_df["CPA"] = caption_df.apply(
                lambda r: (r["Cost"] / r["CV件数"]) if pd.notna(r["CV件数"]) and r["CV件数"] > 0 else pd.NA,
                axis=1,
            )

            image_df = (
                image_df.merge(
                    caption_df[
                        ["CampaignId", "AdName", "Cost", "Impressions", "Clicks", "CV件数", "CTR", "CPA"]
                    ],
                    on=["CampaignId", "AdName"],
                    how="left",
                )
                .drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])
            )

            sort_option = st.radio("並び替え基準", ["AdNum", "CV件数(多)", "CPA(小)"])
            if sort_option == "CV件数(多)":
                image_df = image_df[image_df["CV件数"] > 0].sort_values("CV件数", ascending=False)
            elif sort_option == "CPA(小)":
                image_df = image_df[image_df["CPA"].notna()].sort_values("CPA")
            else:
                image_df = image_df.sort_values("AdNum")

            caption_map = caption_df.set_index(["CampaignId", "AdName"]).to_dict("index")

            if image_df.empty:
                st.warning("⚠️ 表示できる画像がありません")
            else:
                cols = st.columns(5, gap="small")  # 5 枚ずつ＆隙間を詰める
                for i, (_, row) in enumerate(image_df.iterrows()):
                    adname = row["AdName"]
                    campid = row["CampaignId"]
                    v = caption_map.get((campid, adname), {})
                    cost, imp, clicks = v.get("Cost", 0), v.get("Impressions", 0), v.get("Clicks", 0)
                    ctr, cpa, cv = v.get("CTR"), v.get("CPA"), v.get("CV件数", 0)
                    text = latest_text_map.get(adname, "")

                    caption = f"""
                      <div class='ad-caption'>
                        <b>広告名：</b>{adname}<br>
                        <b>消化金額：</b>{cost:,.0f}円<br>
                        <b>IMP：</b>{imp:,.0f}<br>
                        <b>クリック：</b>{clicks:,.0f}<br>
                        <b>CTR：</b>{ctr*100:.2f}%<br>" if ctr not in [None, pd.NA] else "<b>CTR：</b>-<br>"
                        <b>CV数：</b>{int(cv) if cv > 0 else 'なし'}<br>
                        <b>CPA：</b>{cpa:,.0f}円<br>" if pd.notna(cpa) else "<b>CPA：</b>-<br>"
                        <b>メインテキスト：</b>{text}
                      </div>
                    """

                    card_html = f"""
                      <div class='ad-card'>
                        <img src="{row['CloudStorageUrl']}">
                        {caption}
                      </div>
                    """

                    with cols[i % 5]:
                        st.markdown(card_html, unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
