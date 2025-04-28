# 08_⚙️Client_Settings.py
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="Client設定", layout="wide")
st.title("⚙️ Client設定")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- テーブル情報 ---
project = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "ClientSettings"
full_table = f"{project}.{dataset}.{table}"

# --- データ取得 ---
@st.cache_data(ttl=60)
def load_clients():
    query = f"SELECT * FROM `{full_table}`"
    return client.query(query).to_dataframe()

client_settings_df = load_clients()

# --- 登録エリア ---
st.markdown("### ➕ クライアントを新規登録")

# Final_Ad_Dataから取得
@st.cache_data(ttl=60)
def get_unique_clients():
    query = f"""
    SELECT DISTINCT client_name
    FROM `{project}.{dataset}.Final_Ad_Data`
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

try:
    all_clients_df = get_unique_clients()
    already_registered = set(client_settings_df["client_name"]) if not client_settings_df.empty else set()
    unregistered_df = all_clients_df[~all_clients_df["client_name"].isin(already_registered)]

    if unregistered_df.empty:
        st.info("✨ すべてのクライアントが登録されています。")
    else:
        selected_client = st.selectbox("🏢 クライアント名を選択", unregistered_df["client_name"])
        input_client_id = st.text_input("🆔 固有ID (英数字推奨)")
        input_building = st.text_input("🏠 棟数 (例: 5棟)" )
        input_business = st.text_input("🛠️ 事業内容 (例: 建売住宅)")
        input_focus = st.text_input("🔥 注力度 (例: 高)" )

        if st.button("＋ 登録する"):
            if selected_client and input_client_id:
                new_row = pd.DataFrame([{
                    "client_name": selected_client,
                    "client_id": input_client_id,
                    "building_count": input_building,
                    "buisiness_content": input_business,
                    "focus_level": input_focus,
                    "created_at": datetime.now()
                }])
                updated_df = pd.concat([client_settings_df, new_row], ignore_index=True)

                try:
                    with st.spinner("保存中です..."):
                        job_config = bigquery.LoadJobConfig(
                            write_disposition="WRITE_TRUNCATE",
                            schema=[
                                bigquery.SchemaField("client_name", "STRING"),
                                bigquery.SchemaField("client_id", "STRING"),
                                bigquery.SchemaField("building_count", "STRING"),
                                bigquery.SchemaField("buisiness_content", "STRING"),
                                bigquery.SchemaField("focus_level", "STRING"),
                                bigquery.SchemaField("created_at", "TIMESTAMP"),
                            ]
                        )
                        job = client.load_table_from_dataframe(updated_df, full_table, job_config=job_config)
                        job.result()
                        st.success(f"✅ {selected_client} を登録しました！")
                        st.cache_data.clear()
                        client_settings_df = load_clients()
                except Exception as e:
                    st.error(f"❌ 保存に失敗しました: {e}")
            else:
                st.warning("⚠️ 固有IDを入力してください。")
except Exception as e:
    st.error(f"❌ クライアント一覧の取得に失敗しました: {e}")

# --- 登録済みクライアント一覧（編集可能） ---
st.markdown("---")
st.markdown("### 📝 登録済みクライアント一覧（編集可能）")

editable_df = st.data_editor(
    client_settings_df,
    use_container_width=True,
    num_rows="dynamic",
    key="editable_client_table"
)

if st.button("💾 修正内容を保存"):
    with st.spinner("修正内容を保存中です..."):
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField("client_name", "STRING"),
                    bigquery.SchemaField("client_id", "STRING"),
                    bigquery.SchemaField("building_count", "STRING"),
                    bigquery.SchemaField("buisiness_content", "STRING"),
                    bigquery.SchemaField("focus_level", "STRING"),
                    bigquery.SchemaField("created_at", "TIMESTAMP"),
                ]
            )
            job = client.load_table_from_dataframe(editable_df, full_table, job_config=job_config)
            job.result()
            st.success("✅ 編集した内容を保存しました！")
            st.cache_data.clear()
            client_settings_df = load_clients()
        except Exception as e:
            st.error(f"❌ 編集内容の保存に失敗しました: {e}")

# --- ページリンク一覧 ---
st.markdown("---")
st.markdown("### 🌐 クライアント別ページリンク")

for _, row in client_settings_df.iterrows():
    client_name = row["client_name"]
    client_id = row["client_id"]
    page_url = f"https://{st.secrets['app_domain']}/1_Main_Dashboard?client_id={client_id}"
    st.markdown(f"[{client_name}専用ページを開く]({page_url})", unsafe_allow_html=True)
