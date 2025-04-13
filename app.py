import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 Final_Ad_Data Dashboard")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# クエリ実行
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

        # カテゴリ・Date の下処理
        if "カテゴリ" in df.columns:
            df["カテゴリ"] = df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # サイドバー フィルター
        st.sidebar.header("🔍 フィルター")

        # 上から順にフィルター用データを絞る
        selected_client = st.sidebar.selectbox("クライアント", ["すべて"] + sorted(df["PromotionName"].dropna().unique()))
        df_by_client = df if selected_client == "すべて" else df[df["PromotionName"] == selected_client]

        selected_category = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(df_by_client["カテゴリ"].dropna().unique()))
        df_by_category = df_by_client if selected_category == "すべて" else df_by_client[df_by_client["カテゴリ"] == selected_category]

        selected_campaign = st.sidebar.selectbox("キャンペーン名", ["すべて"] + sorted(df_by_category["CampaignName"].dropna().unique()))

        if "Date" in df.columns and not df["Date"].isnull().all():
            # クライアント・カテゴリの絞り込み後データを使う（キャンペーン名はまだ未選択なので）
            date_base_df = df_by_category

            # nullを除いたうえで範囲を指定
            valid_dates = date_base_df["Date"].dropna()
            if not valid_dates.empty:
                min_date, max_date = valid_dates.min(), valid_dates.max()
                selected_date = st.sidebar.date_input("日付", [min_date, max_date], min_value=min_date, max_value=max_date)
            else:
                selected_date = None

        # 絞り込み
        filtered_df = df.copy()
        if selected_client != "すべて":
            filtered_df = filtered_df[filtered_df["PromotionName"] == selected_client]
        if selected_category != "すべて":
            filtered_df = filtered_df[filtered_df["カテゴリ"] == selected_category]
        if selected_campaign != "すべて":
            filtered_df = filtered_df[filtered_df["CampaignName"] == selected_campaign]
        if isinstance(selected_date, list) and len(selected_date) == 2:
            start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
            filtered_df = filtered_df[filtered_df["Date"].notna()]
            filtered_df = filtered_df[(filtered_df["Date"] >= start_date) & (filtered_df["Date"] <= end_date)]

        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        st.subheader("🖼️ 画像ギャラリー【CloudStorageUrl】")
        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🌟 CloudStorageUrl から画像を取得中...")

            # 画像がある行だけ抽出
            image_df = filtered_df[filtered_df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()

            # 型・前後スペースを整える（マージのため）
            image_df["AdName"] = image_df["AdName"].astype(str).str.strip()
            image_df["CampaignId"] = image_df["CampaignId"].astype(str).str.strip()
            image_df["CloudStorageUrl"] = image_df["CloudStorageUrl"].astype(str).str.strip()
            image_df["AdNum"] = pd.to_numeric(image_df["AdName"], errors="coerce")
            image_df = image_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            # 数値列
            for col in ["Cost", "Impressions", "Clicks"]:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")

            # CV列（1〜60）を無い場合は0で作る
            for i in range(1, 61):
                col = str(i)
                if col not in filtered_df.columns:
                    filtered_df[col] = 0

            # CV件数を AdNum から取得
            def get_cv(row):
                adnum = row["AdNum"]
                if pd.isna(adnum):
                    return 0
                return row.get(str(int(adnum)), 0)

            image_df["CV件数"] = image_df.apply(get_cv, axis=1)

            # 最新のDescription1ByAdTypeを取る
            latest_rows = image_df.sort_values("Date").dropna(subset=["Date"])
            latest_rows = latest_rows.loc[latest_rows.groupby("AdName")["Date"].idxmax()]
            latest_text_map = latest_rows.set_index("AdName")["Description1ByAdType"].to_dict()

            # 集計用DataFrame
            agg_df = filtered_df.copy()
            agg_df["AdName"] = agg_df["AdName"].astype(str).str.strip()
            agg_df["CampaignId"] = agg_df["CampaignId"].astype(str).str.strip()
            agg_df["AdNum"] = pd.to_numeric(agg_df["AdName"], errors="coerce")
            agg_df = agg_df[agg_df["AdNum"].notna()]
            agg_df["AdNum"] = agg_df["AdNum"].astype(int)

            # CV集計
            cv_sum_df = image_df.groupby(["CampaignId", "AdName"])["CV件数"].sum().reset_index()

            # コスト等を広告単位で合計 → CV件数 をマージ → CTR, CPA を計算
            caption_df = agg_df.groupby(["CampaignId", "AdName"]).agg({
                "Cost": "sum",
                "Impressions": "sum",
                "Clicks": "sum"
            }).reset_index()
            caption_df = caption_df.merge(cv_sum_df, on=["CampaignId", "AdName"], how="left")

            caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
            caption_df["CPA"] = caption_df.apply(
                lambda row: (row["Cost"] / row["CV件数"]) if pd.notna(row["CV件数"]) and row["CV件数"] > 0 else pd.NA,
                axis=1
            )

            # ---------------------------
            # 常に再マージして CPA を更新
            # ---------------------------
            # 既にCPA列があっても上書き
            image_df.drop(
                columns=["Cost", "Impressions", "Clicks", "CV件数", "CTR", "CPA"],
                errors="ignore",
                inplace=True
            )
            image_df = image_df.merge(
                caption_df[["CampaignId", "AdName", "Cost", "Impressions", "Clicks", "CV件数", "CTR", "CPA"]],
                on=["CampaignId", "AdName"],
                how="left"
            )

            # 並び替えコントロール
            sort_option = st.radio("並び替え基準", ["AdNum", "CV件数(多)", "CPA(小)"])

            if sort_option == "CV件数(多)":
                # CV件数=0 → 視覚的に「CV数: なし」は除外
                image_df = image_df[image_df["CV件数"] > 0]
                image_df = image_df.sort_values(by="CV件数", ascending=False)
            elif sort_option == "CPA(小)":
                # CPA=NaN → 視覚的に「CPA: -」は除外
                image_df = image_df[image_df["CPA"].notna()]
                image_df = image_df.sort_values(by="CPA", ascending=True)
            else:
                image_df = image_df.sort_values("AdNum")

            # caption_df を lookup 用にdict化
            caption_map = caption_df.set_index(["CampaignId", "AdName"]).to_dict("index")

            if image_df.empty:
                st.warning("⚠️ 表示できる画像がありません")
            else:
                cols = st.columns(5)
                for i, (_, row) in enumerate(image_df.iterrows()):
                    adname = row["AdName"]
                    campid = row["CampaignId"]
                    values = caption_map.get((campid, adname), {})
                    cost = values.get("Cost", 0)
                    imp = values.get("Impressions", 0)
                    clicks = values.get("Clicks", 0)
                    ctr = values.get("CTR")
                    cpa = values.get("CPA", None)
                    cv = values.get("CV件数", 0)
                    text = latest_text_map.get(adname, "")

                    caption_html = f"""
                    <div style='text-align: left; font-size: 14px; line-height: 1.6'>
                    <b>広告名：</b>{adname}<br>
                    <b>消化金額：</b>{cost:,.0f}円<br>
                    <b>IMP：</b>{imp:,.0f}<br>
                    <b>クリック：</b>{clicks:,.0f}<br>
                    """
                    caption_html += f"<b>CTR：</b>{ctr*100:.2f}%<br>" if pd.notna(ctr) else "<b>CTR：</b>-<br>"
                    caption_html += f"<b>CV数：</b>{int(cv) if cv > 0 else 'なし'}<br>"
                    caption_html += f"<b>CPA：</b>{cpa:,.0f}円<br>" if (pd.notna(cpa)) else "<b>CPA：</b>-<br>"
                    caption_html += f"<b>メインテキスト：</b>{text}</div>"

                    with cols[i % 5]:
                        st.image(row["CloudStorageUrl"], use_container_width=True)
                        st.markdown(caption_html, unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
