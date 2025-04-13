import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.title("📊 Final_Ad_Data Dashboard")

# 1) まずは info_dict を作成
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")

# 3) BigQuery クライアント作成
client = bigquery.Client.from_service_account_info(info_dict)

# クエリ
query = """
SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`
LIMIT 1000
"""
st.write("🔄 データを取得中...")

try:
    df = client.query(query).to_dataframe()

    if df.empty:
        st.warning("⚠️ データがありません")
    else:
        st.success("✅ データ取得成功！")

        # ✅ カテゴリの空白・欠損を「未設定」に統一
        if "カテゴリ" in df.columns:
            df["カテゴリ"] = df["カテゴリ"].astype(str).str.strip()
            df["カテゴリ"] = df["カテゴリ"].replace("", "未設定")
            df["カテゴリ"] = df["カテゴリ"].fillna("未設定")

        # ✅ 日付をdatetime型に変換（必要なら）
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # ==============================
        # 🔍 サイドバーでフィルター
        # ==============================
        st.sidebar.header("🔍 フィルター")

        client_options = ["すべて"] + sorted(df["PromotionName"].dropna().unique())
        selected_client = st.sidebar.selectbox("クライアントで絞り込み", client_options)

        category_options = ["すべて"] + sorted(df["カテゴリ"].unique())
        selected_category = st.sidebar.selectbox("カテゴリで絞り込み", category_options)

        campaign_options = ["すべて"] + sorted(df["CampaignName"].dropna().unique())
        selected_campaign = st.sidebar.selectbox("キャンペーン名で絞り込み", campaign_options)

        if "Date" in df.columns and not df["Date"].isnull().all():
            min_date = df["Date"].min()
            max_date = df["Date"].max()
            selected_date = st.sidebar.date_input("日付で絞り込み", [min_date, max_date])

        # ==============================
        # 🎯 絞り込み処理
        # ==============================
        filtered_df = df.copy()

        if selected_client != "すべて":
            filtered_df = filtered_df[filtered_df["PromotionName"] == selected_client]
        if selected_category != "すべて":
            filtered_df = filtered_df[filtered_df["カテゴリ"] == selected_category]
        if selected_campaign != "すべて":
            filtered_df = filtered_df[filtered_df["CampaignName"] == selected_campaign]
        if "Date" in df.columns and isinstance(selected_date, list) and len(selected_date) == 2:
            start_date, end_date = pd.to_datetime(selected_date)
            filtered_df = filtered_df[
                (filtered_df["Date"] >= start_date) & (filtered_df["Date"] <= end_date)
            ]

        # ==============================
        # 📋 表の表示
        # ==============================
        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        # ==============================
        # 🖼️ 画像ギャラリー
        # ==============================
        st.subheader("🖼️ 画像ギャラリー（CloudStorageUrl）")

        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🎯 CloudStorageUrl から画像を取得中...")
            cols = st.columns(5)

            for i, (_, row) in enumerate(filtered_df.iterrows()):
                url = row["CloudStorageUrl"]
                # 無効なURL（None/空文字/ゼロ/非http）を除外
                if (
                    isinstance(url, str) and
                    url.strip() != "" and
                    url != "0" and
                    url.startswith("http")
                ):
                    with cols[i % 5]:
                        st.image(
                            url,
                            caption=row.get("canvaURL", "（canvaURLなし）"),
                            use_container_width=True
                        )
        else:
            st.warning("⚠️ CloudStorageUrl 列が見つかりません")

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
