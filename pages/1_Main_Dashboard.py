import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="メインダッシュボード", layout="wide")
st.title("📊 Final_Ad_Data Dashboard")

# 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

query = """
SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
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

        if "Date" in df.columns and not df["Date"].isnull().all():
            min_date = df["Date"].min().date()
            max_date = df["Date"].max().date()
            selected_date = st.sidebar.date_input("日付フィルター", (min_date, max_date), min_value=min_date, max_value=max_date)
            if isinstance(selected_date, (list, tuple)) and len(selected_date) == 2:
                start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
                date_filtered_df = df[(df["Date"].dt.date >= start_date.date()) & (df["Date"].dt.date <= end_date.date())]
            else:
                date_filtered_df = df[df["Date"].dt.date == pd.to_datetime(selected_date).date()]
        else:
            date_filtered_df = df.copy()

        st.sidebar.header("🔍 フィルター")
        all_clients = sorted(date_filtered_df["PromotionName"].dropna().unique())

        def update_client():
            cs = st.session_state.client_search
            if cs in all_clients:
                st.session_state.selected_client = cs

        client_search = st.sidebar.text_input("クライアント検索", "", placeholder="必ず正しく入力してEnterを押す", key="client_search", on_change=update_client)
        filtered_clients = [client for client in all_clients if client_search.lower() in client.lower()] if client_search else all_clients
        client_options = ["すべて"] + filtered_clients
        default_index = client_options.index(st.session_state.get("selected_client", "すべて")) if st.session_state.get("selected_client", "すべて") in client_options else 0
        selected_client = st.sidebar.selectbox("クライアント", client_options, index=default_index)
        client_filtered_df = date_filtered_df if selected_client == "すべて" else date_filtered_df[date_filtered_df["PromotionName"] == selected_client]

        selected_category = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(client_filtered_df["カテゴリ"].dropna().unique()))
        client_cat_filtered_df = client_filtered_df if selected_category == "すべて" else client_filtered_df[client_filtered_df["カテゴリ"] == selected_category]

        selected_campaign = st.sidebar.selectbox("キャンペーン名", ["すべて"] + sorted(client_cat_filtered_df["CampaignName"].dropna().unique()))
        filtered_df = client_cat_filtered_df if selected_campaign == "すべて" else client_cat_filtered_df[client_cat_filtered_df["CampaignName"] == selected_campaign]

        st.subheader("📋 表形式データ")
        st.dataframe(filtered_df)

        for i in range(1, 61):
            col = str(i)
            filtered_df[col] = pd.to_numeric(filtered_df.get(col, 0), errors="coerce").fillna(0)

        st.subheader("📈 広告パフォーマンス概要")

        try:
            filtered_df["Cost"] = pd.to_numeric(filtered_df["Cost"], errors="coerce").fillna(0)
            filtered_df["Impressions"] = pd.to_numeric(filtered_df["Impressions"], errors="coerce").fillna(0)
            filtered_df["Clicks"] = pd.to_numeric(filtered_df["Clicks"], errors="coerce").fillna(0)
            filtered_df["予算"] = filtered_df["予算"].apply(
                lambda x: pd.to_numeric(x) if pd.notna(x) and str(x).strip().isdigit() else pd.NA
            )
            filtered_df["Reach"] = pd.to_numeric(filtered_df.get("Reach", 0), errors="coerce").fillna(0)

            cv_df = filtered_df.dropna(subset=["CampaignId", "Date"]).copy()
            cv_df["コンバージョン数"] = cv_df["コンバージョン数"].apply(
                lambda x: pd.to_numeric(x) if pd.notna(x) and str(x).strip().isdigit() else pd.NA
            )
            latest_cv_df = cv_df.sort_values("Date").groupby("CampaignId", as_index=False).last()
            total_cv = latest_cv_df["コンバージョン数"].sum(min_count=1)

            budget_df = filtered_df.dropna(subset=["CampaignId", "Date"]).copy()
            budget_df["予算"] = budget_df["予算"].apply(
                lambda x: pd.to_numeric(x) if pd.notna(x) and str(x).strip().isdigit() else pd.NA
            )
            latest_budget_df = budget_df.sort_values("Date").groupby("CampaignId", as_index=False).last()
            total_budget = latest_budget_df["予算"].sum(min_count=1)

            total_cost = filtered_df["Cost"].sum()
            total_clicks = filtered_df["Clicks"].sum()
            total_impressions = filtered_df["Impressions"].sum()
            total_reach = filtered_df["Reach"].sum()

            cpa_by_cost = total_cost / total_cv if total_cv and total_cv > 0 else None
            cpa_by_budget = total_budget / total_cv if total_cv and total_cv > 0 else None
            ctr = total_clicks / total_impressions if total_impressions > 0 else None
            cvr = total_clicks / total_cv if total_cv and total_cv > 0 else None
            cpc = total_cost / total_clicks if total_clicks > 0 else None
            cpm = (total_cost / total_impressions) * 1000 if total_impressions > 0 else None
            freq = total_impressions / total_reach if total_reach > 0 else None

            summary_data = {
                "指標": [
                    "CPA - 獲得単価（消化金額）",
                    "CPA - 獲得単価（予算）",
                    "コンバージョン数",
                    "CVR - コンバージョン率",
                    "消化金額",
                    "インプレッション",
                    "CTR - クリック率",
                    "CPC - クリック単価",
                    "クリック数",
                    "CPM",
                    "フリークエンシー"
                ],
                "値": [
                    f"{cpa_by_cost:,.0f} 円" if cpa_by_cost is not None else "-",
                    f"{cpa_by_budget:,.0f} 円" if cpa_by_budget is not None else "-",
                    f"{int(total_cv):,}" if total_cv and total_cv > 0 else "0",
                    f"{cvr:.2%}" if cvr is not None else "-",
                    f"{total_cost:,.0f} 円",
                    f"{int(total_impressions):,}",
                    f"{ctr:.2%}" if ctr is not None else "-",
                    f"{cpc:,.0f} 円" if cpc is not None else "-",
                    f"{int(total_clicks):,}",
                    f"{cpm:,.0f} 円" if cpm is not None else "-",
                    f"{freq:.2f}" if freq is not None else "-"
                ]
            }

            perf_df = pd.DataFrame(summary_data)
            st.dataframe(perf_df, hide_index=True, use_container_width=True)

        except Exception as e:
            st.error(f"❌ 指標の集計エラー: {e}")

        st.subheader("🖼️ 配信バナー")
        if "CloudStorageUrl" in filtered_df.columns:
            st.write("🌟 CloudStorageUrl から画像を取得中...")
            image_df = filtered_df[filtered_df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()
            image_df["AdName"] = image_df["AdName"].astype(str).str.strip()
            image_df["CampaignId"] = image_df["CampaignId"].astype(str).str.strip()
            image_df["CloudStorageUrl"] = image_df["CloudStorageUrl"].astype(str).str.strip()
            image_df["AdNum"] = pd.to_numeric(image_df["AdName"], errors="coerce")
            image_df = image_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            def get_cv(row):
                adnum = row["AdNum"]
                if pd.isna(adnum):
                    return 0
                col_name = str(int(adnum))
                return row[col_name] if col_name in row and isinstance(row[col_name], (int, float)) else 0

            image_df["CV件数"] = image_df.apply(get_cv, axis=1)

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
            caption_df = agg_df.groupby(["CampaignId", "AdName"]).agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"}).reset_index()
            caption_df = caption_df.merge(cv_sum_df, on=["CampaignId", "AdName"], how="left")
            caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
            caption_df["CPA"] = caption_df.apply(lambda row: row["Cost"] / row["CV件数"] if pd.notna(row["CV件数"]) and row["CV件数"] > 0 else pd.NA, axis=1)

            image_df = image_df.merge(caption_df[["CampaignId", "AdName", "Cost", "Impressions", "Clicks", "CV件数", "CTR", "CPA"]], on=["CampaignId", "AdName"], how="left")

            sort_option = st.radio("並び替え基準", ["AdNum", "CV件数(多)", "CPA(小)"])
            if sort_option == "CV件数(多)":
                image_df = image_df[image_df["CV件数"] > 0].sort_values(by="CV件数", ascending=False)
            elif sort_option == "CPA(小)":
                image_df = image_df[image_df["CPA"].notna()].sort_values(by="CPA", ascending=True)
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
                    cpa = values.get("CPA")
                    cv = values.get("CV件数", 0)
                    text = latest_text_map.get(adname, "")

                    with cols[i % 5]:
                        st.markdown(f"""
                            <div style='border: 1px solid #ddd; border-radius: 8px; padding: 10px; margin-bottom: 20px; height: 520px; display: flex; flex-direction: column; justify-content: flex-start; align-items: center; background-color: #f9f9f9;'>
                                <a href="{row['CloudStorageUrl']}" target="_blank" style="width: 100%; height: 220px; overflow: hidden; display: flex; align-items: center; justify-content: center;">
                                    <img src="{row['CloudStorageUrl']}" style="max-width: 100%; max-height: 100%; object-fit: contain;" />
                                </a>
                                <div style='text-align: left; font-size: 14px; line-height: 1.6; padding-top: 10px; width: 100%;'>
                                    <b>広告名：</b>{adname}<br>
                                    <b>消化金額：</b>{cost:,.0f}円<br>
                                    <b>IMP：</b>{imp:,.0f}<br>
                                    <b>クリック：</b>{clicks:,.0f}<br>
                                    {'<b>CTR：</b>{:.2f}%<br>'.format(ctr*100) if pd.notna(ctr) else '<b>CTR：</b>-<br>'}
                                    <b>CV数：</b>{int(cv) if cv > 0 else 'なし'}<br>
                                    <b>CPA：</b>{f"{cpa:,.0f}円" if pd.notna(cpa) else '-'}<br>
                                    <b>メインテキスト：</b>{text}<br>
                                </div>
                            </div>
                        """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
