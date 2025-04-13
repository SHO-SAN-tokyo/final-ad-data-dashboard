import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(layout="wide")  # Wideモード
st.title("📊 Final_Ad_Data Dashboard")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
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

        if "カテゴリ" in df.columns:
            df["カテゴリ"] = df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        st.sidebar.header("🔍 フィルター")
        selected_client = st.sidebar.selectbox("クライアント", ["すべて"] + sorted(df["PromotionName"].dropna().unique()))
        selected_category = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(df["カテゴリ"].unique()))
        selected_campaign = st.sidebar.selectbox("キャンペーン名", ["すべて"] + sorted(df["CampaignName"].dropna().unique()))

        if "Date" in df.columns and not df["Date"].isnull().all():
            min_date, max_date = df["Date"].min(), df["Date"].max()
            selected_date = st.sidebar.date_input("日付", [min_date, max_date])

        filtered_df = df.copy()
        if selected_client != "すべて":
            filtered_df = filtered_df[filtered_df["PromotionName"] == selected_client]
        if selected_category != "すべて":
            filtered_df = filtered_df[filtered_df["カテゴリ"] == selected_category]
        if selected_campaign != "すべて":
            filtered_df = filtered_df[filtered_df["CampaignName"] == selected_campaign]
        if isinstance(selected_date, list) and len(selected_date) == 2:
            start_date, end_date = pd.to_datetime(selected_date)
            filtered_df = filtered_df[(filtered_df["Date"] >= start_date) & (filtered_df["Date"] <= end_date)]

        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        st.subheader("🖼️ 画像ギャラリー（CloudStorageUrl）")
        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🎯 CloudStorageUrl から画像を取得中...")

            image_df = filtered_df[filtered_df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
            image_df["AdName"] = image_df["AdName"].astype(str).str.strip()
            image_df["CampaignId"] = image_df["CampaignId"].astype(str).str.strip()
            image_df["CloudStorageUrl"] = image_df["CloudStorageUrl"].astype(str).str.strip()
            image_df["AdNum"] = pd.to_numeric(image_df["AdName"], errors="coerce")

            image_df = image_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            # 集計用に数値変換
            for col in ["Cost", "Impressions", "Clicks"]:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")

            for i in range(1, 61):
                col = str(i)
                if col not in filtered_df.columns:
                    filtered_df[col] = 0

            def get_cv(row):
                adnum = row["AdNum"]
                if pd.isna(adnum): return 0
                return row.get(str(int(adnum)), 0)
            image_df["CV件数"] = image_df.apply(get_cv, axis=1)

            # 最新のDescription取得
            latest_rows = image_df.sort_values("Date").dropna(subset=["Date"])
            latest_rows = latest_rows.loc[latest_rows.groupby("AdName")["Date"].idxmax()]
            latest_text_map = latest_rows.set_index("AdName")["Description1ByAdType"].to_dict()

            if image_df.empty:
                st.warning("⚠️ 表示できる画像がありません")
            else:
                image_df = image_df.sort_values("AdNum")
                caption_df = image_df.groupby("AdName").agg({
                    "Cost": "sum",
                    "Impressions": "sum",
                    "Clicks": "sum",
                    "CV件数": "sum"
                }).reset_index()

                caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
                caption_df["CPA"] = caption_df["Cost"] / caption_df["CV件数"].replace(0, pd.NA)
                caption_map = caption_df.set_index("AdName").to_dict("index")

                cols = st.columns(5)
                for i, (_, row) in enumerate(image_df.iterrows()):
                    adname = row["AdName"]
                    values = caption_map.get(adname, {})
                    caption = f"広告名：{adname}\n"
                    caption += f"消化金額：{values.get('Cost', 0):,.0f}円\n"
                    caption += f"IMP：{values.get('Impressions', 0):,.0f}\n"
                    caption += f"クリック：{values.get('Clicks', 0):,.0f}\n"
                    ctr = values.get("CTR")
                    caption += f"CTR：{ctr * 100:.2f}%\n" if pd.notna(ctr) else "CTR：-\n"
                    caption += f"CV数：{values.get('CV件数', 0):,.0f}件\n"
                    cpa = values.get("CPA")
                    caption += f"CPA：{cpa:,.0f}円\n" if pd.notna(cpa) else "CPA：-\n"
                    caption += f"メインテキスト：{latest_text_map.get(adname, '')}"

                    with cols[i % 5]:
                        st.image(
                            row["CloudStorageUrl"],
                            caption=caption,
                            use_container_width=True
                        )
        else:
            st.warning("⚠️ CloudStorageUrl 列が見つかりません")

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
