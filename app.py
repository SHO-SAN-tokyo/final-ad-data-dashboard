import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 Final_Ad_Data Dashboard")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# クエリ実行（必要に応じて LIMIT を外すか WHERE 句で期間指定するのも検討）
query = """
SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
LIMIT 1000
"""
st.write("🔄 データを取得中...")

try:
    df = client.query(query).to_dataframe()
    
    if df.empty:
        st.warning("⚠️ データがありません")
    else:
        st.success("✅ データ取得成功！")

        # 前処理：カテゴリと日付の型変換（全件に対して適用）
        if "カテゴリ" in df.columns:
            df["カテゴリ"] = df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        
        # -------------------------------------
        # ① 日付フィルタの適用（全体のデータから最小・最大を取得）
        # -------------------------------------
        if "Date" in df.columns and not df["Date"].isnull().all():
            min_date = df["Date"].min().date()
            max_date = df["Date"].max().date()
            selected_date = st.sidebar.date_input(
                "日付フィルター",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
                start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
                date_filtered_df = df[(df["Date"].dt.date >= start_date.date()) & (df["Date"].dt.date <= end_date.date())]
            else:
                date_filtered_df = df[df["Date"].dt.date == pd.to_datetime(selected_date).date()]
        else:
            date_filtered_df = df.copy()

        st.sidebar.header("🔍 フィルター")
        
        # -------------------------------------
        # ② クライアントフィルター（検索付き＋Enterで選択反映）
        # -------------------------------------
        # 全体のクライアント一覧を取得
        all_clients = sorted(date_filtered_df["PromotionName"].dropna().unique())

        # コールバック関数: 入力された値が全体リストに存在する場合、セッションステートを更新
        def update_client():
            cs = st.session_state.client_search
            if cs in all_clients:
                st.session_state.selected_client = cs

        client_search = st.sidebar.text_input(
            "クライアント検索",
            "",
            placeholder="必ず正しく入力してEnterを押す",
            key="client_search",
            on_change=update_client
        )
        if client_search:
            filtered_clients = [client for client in all_clients if client_search.lower() in client.lower()]
        else:
            filtered_clients = all_clients

        # Selectbox 用のオプションは、["すべて"] + 現在の候補リストとする
        client_options = ["すべて"] + filtered_clients
        # セッションステート内の "selected_client" を取得
        selected_client_in_state = st.session_state.get("selected_client", "すべて")
        if selected_client_in_state in client_options:
            default_index = client_options.index(selected_client_in_state)
        else:
            default_index = 0

        selected_client = st.sidebar.selectbox("クライアント", client_options, index=default_index)

        # クライアントの選択に応じた一時的なデータフレームを作成
        if selected_client != "すべて":
            client_filtered_df = date_filtered_df[date_filtered_df["PromotionName"] == selected_client]
        else:
            client_filtered_df = date_filtered_df.copy()
        
        # -------------------------------------
        # ③ カテゴリフィルター（クライアントフィルター後の候補リスト）
        # -------------------------------------
        selected_category = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(client_filtered_df["カテゴリ"].dropna().unique()))
        if selected_category != "すべて":
            client_cat_filtered_df = client_filtered_df[client_filtered_df["カテゴリ"] == selected_category]
        else:
            client_cat_filtered_df = client_filtered_df.copy()

        # -------------------------------------
        # ④ キャンペーン名フィルター（クライアント＆カテゴリフィルタ後の候補リスト）
        # -------------------------------------
        selected_campaign = st.sidebar.selectbox("キャンペーン名", ["すべて"] + sorted(client_cat_filtered_df["CampaignName"].dropna().unique()))
        filtered_df = client_cat_filtered_df.copy()
        if selected_campaign != "すべて":
            filtered_df = filtered_df[filtered_df["CampaignName"] == selected_campaign]

        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        # --- デバッグ用表示（不要な場合はコメントアウト） ---
        # st.write("### filtered_df のカラム一覧")
        # st.write(filtered_df.columns.tolist())
        # st.write("### filtered_df の先頭5行")
        # st.dataframe(filtered_df.head())
        # ---------------------------------------------------------

        # filtered_df に全件補完用のカラム（"1"～"60"）を追加
        for i in range(1, 61):
            col = str(i)
            if col not in filtered_df.columns:
                filtered_df[col] = 0

        # -------------------------------------
        # ⑤ 画像表示・集計処理（filtered_df を基に実施）
        # -------------------------------------
        st.subheader("🖼️ 画像ギャラリー【CloudStorageUrl】")
        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🌟 CloudStorageUrl から画像を取得中...")
            
            # image_df は filtered_df から作成するので、補完済みのカラムも引き継ぐ
            image_df = filtered_df[filtered_df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()

            # --- デバッグ用表示（不要な場合はコメントアウト） ---
            st.write("### image_df のカラム一覧")
            st.write(image_df.columns.tolist())
            st.write("### image_df の先頭5行")
            st.dataframe(image_df.head())
            # ---------------------------------------------------------

            image_df["AdName"] = image_df["AdName"].astype(str).str.strip()
            image_df["CampaignId"] = image_df["CampaignId"].astype(str).str.strip()
            image_df["CloudStorageUrl"] = image_df["CloudStorageUrl"].astype(str).str.strip()
            image_df["AdNum"] = pd.to_numeric(image_df["AdName"], errors="coerce")
            image_df = image_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            for col in ["Cost", "Impressions", "Clicks"]:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")

            # get_cv関数: 「AdNum」で示された列名（文字列）をもとに、その値を取得する
            def get_cv(row):
                adnum = row["AdNum"]
                if pd.isna(adnum):
                    return 0
                col_name = str(int(adnum))   # 例："10"
                return row[col_name] if (col_name in row and isinstance(row[col_name], (int, float))) else 0

            image_df["CV件数"] = image_df.apply(get_cv, axis=1)

            # --- デバッグ用: 特定の行の get_cv の結果を確認  ---
            if not image_df.empty:
                st.write("### 先頭行の get_cv の返り値")
                st.write(get_cv(image_df.iloc[0]))
            # -----------------------------------------------------

            latest_rows = image_df.sort_values("Date").dropna(subset=["Date"])
            latest_rows = latest_rows.loc[latest_rows.groupby("AdName")["Date"].idxmax()]
            latest_text_map = latest_rows.set_index("AdName")["Description1ByAdType"].to_dict()

            agg_df = filtered_df.copy()
            agg_df["AdName"] = agg_df["AdName"].astype(str).str.strip()
            agg_df["CampaignId"] = agg_df["CampaignId"].astype(str).str.strip()
            agg_df["AdNum"] = pd.to_numeric(agg_df["AdName"], errors="coerce")
            agg_df = agg_df[agg_df["AdNum"].notna()]
            agg_df["AdNum"] = agg_df["AdNum"].astype(int)

            cv_sum_df = image_df.groupby(["CampaignId", "AdName"])["CV件数"].sum().reset_index()

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

            sort_option = st.radio("並び替え基準", ["AdNum", "CV件数(多)", "CPA(小)"])

            if sort_option == "CV件数(多)":
                image_df = image_df[image_df["CV件数"] > 0]
                image_df = image_df.sort_values(by="CV件数", ascending=False)
            elif sort_option == "CPA(小)":
                image_df = image_df[image_df["CPA"].notna()]
                image_df = image_df.sort_values(by="CPA", ascending=True)
            else:
                image_df = image_df.sort_values("AdNum")

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
                    caption_html += f"<b>CPA：</b>{cpa:,.0f}円<br>" if pd.notna(cpa) else "<b>CPA：</b>-<br>"
                    caption_html += f"<b>メインテキスト：</b>{text}</div>"

                    with cols[i % 5]:
                        st.image(row["CloudStorageUrl"], use_container_width=True)
                        st.markdown(caption_html, unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
