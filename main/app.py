import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.title("📊 Final_Ad_Data Dashboard")

# 1) info_dict を作成して改行整形 
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")

# 2) BigQuery クライアント作成
client = bigquery.Client.from_service_account_info(info_dict)

# 3) クエリ実行
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

        # カテゴリの整形
        if "カテゴリ" in df.columns:
            df["カテゴリ"] = df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")

        # 日付整形
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # 🔍 サイドバーでフィルター
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

        # 🎯 絞り込み処理
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

        # 📋 表の表示
        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        # 🖼️ 画像ギャラリー（重複排除）
        st.subheader("🖼️ 画像ギャラリー（CloudStorageUrl）")
        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🎯 CloudStorageUrl から画像を取得中...")

            image_df = filtered_df[
                filtered_df["CloudStorageUrl"].astype(str).str.startswith("http")
            ].copy()

            image_df["AdName"] = image_df["AdName"].astype(str).str.strip()
            image_df["CampaignId"] = image_df["CampaignId"].astype(str).str.strip()
            image_df["CloudStorageUrl"] = image_df["CloudStorageUrl"].astype(str).str.strip()

            # ✅ CampaignId + AdName + URLの重複を除去
            image_df = image_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            if image_df.empty:
                st.warning("⚠️ 表示できる画像がありません")
            else:
                cols = st.columns(5)
                for i, (_, row) in enumerate(image_df.iterrows()):
                    with cols[i % 5]:
                        st.image(
                            row["CloudStorageUrl"],
                            caption=row.get("canvaURL", "（canvaURLなし）"),
                            use_container_width=True
                        )
        else:
            st.warning("⚠️ CloudStorageUrl 列が見つかりません")

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
