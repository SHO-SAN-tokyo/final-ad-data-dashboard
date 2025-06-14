import streamlit as st
import pandas as pd
import random
import string
from datetime import datetime
from google.cloud import bigquery

# --- ページ設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- テーブル設定 ---
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "ClientSettings"
full_table = f"{project_id}.{dataset}.{table}"

# --- クライアント一覧取得 ---
@st.cache_data(ttl=60)
def load_clients():
    query = f"""
    SELECT DISTINCT client_name 
    FROM `{project_id}.{dataset}.Final_Ad_Data`
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# --- 登録済み設定取得 ---
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{full_table}`"
    return client.query(query).to_dataframe()

# --- ランダムID生成 ---
def generate_random_id(length=30):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

clients_df = load_clients()
settings_df = load_client_settings()
registered_clients = set(settings_df["client_name"]) if not settings_df.empty else set()
unregistered_df = clients_df[~clients_df["client_name"].isin(registered_clients)]

# --- 新規登録エリア ---
st.markdown("### ➕ 新しいクライアントを登録")
if unregistered_df.empty:
    st.info("✅ 登録可能な新規クライアントはありません")
else:
    selected_client = st.selectbox("👤 クライアント名を選択", unregistered_df["client_name"])
    input_id_prefix = st.text_input("🆔 クライアントIDの接頭辞 (例: livebest)")
    building_count = st.text_input("🏠 棟数")
    business_content = st.text_input("💼 事業内容")
    focus_level = st.text_input("🚀 注力度")

    if st.button("＋ クライアントを登録"):
        if selected_client and input_id_prefix:
            final_id = f"{input_id_prefix}_{generate_random_id()}"
            new_row = pd.DataFrame([{
                "client_name": selected_client,
                "client_id": final_id,
                "building_count": building_count,
                "buisiness_content": business_content,
                "focus_level": focus_level,
                "created_at": datetime.now()
            }])
            updated_df = pd.concat([settings_df, new_row], ignore_index=True)
            try:
                with st.spinner("保存中..."):
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
                    st.rerun()
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")
        else:
            st.warning("⚠️ クライアントIDの接頭辞を入力してください")

# --- 編集エリア ---
st.markdown("---")
st.markdown("### ✏️ クライアント情報の編集")

if settings_df.empty:
    st.info("❗ 登録されたクライアントがありません")
else:
    selected_edit_client = st.selectbox("👤 編集するクライアントを選択", settings_df["client_name"])
    row = settings_df[settings_df["client_name"] == selected_edit_client].iloc[0]

    with st.form(key="edit_form"):
        updated_client_id = st.text_input("🆔 クライアントID", value=row["client_id"])
        updated_building_count = st.text_input("🏠 棟数", value=row["building_count"])
        updated_business_content = st.text_input("💼 事業内容", value=row["buisiness_content"])
        updated_focus_level = st.text_input("🚀 注力度", value=row["focus_level"])

        col1, col2 = st.columns([1, 1])
        with col1:
            save_button = st.form_submit_button("💾 保存")
        with col2:
            delete_button = st.form_submit_button("🗑️ 削除")

    if save_button:
        try:
            settings_df.loc[settings_df["client_name"] == selected_edit_client, [
                "client_id", "building_count", "buisiness_content", "focus_level"
            ]] = [updated_client_id, updated_building_count, updated_business_content, updated_focus_level]
            with st.spinner("保存中..."):
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
                job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                job.result()
                st.success("✅ 保存しました")
                st.rerun()
        except Exception as e:
            st.error(f"❌ 保存エラー: {e}")

    if delete_button:
        try:
            settings_df = settings_df[settings_df["client_name"] != selected_edit_client]
            with st.spinner("削除中..."):
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
                job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                job.result()
                st.success("🗑️ 削除しました")
                st.rerun()
        except Exception as e:
            st.error(f"❌ 削除エラー: {e}")

# --- 一覧表示 ---
st.markdown("---")
st.markdown("### 📋 登録済みクライアント一覧")
if not settings_df.empty:
    st.dataframe(settings_df.sort_values("client_name"), use_container_width=True)
else:
    st.info("登録データがありません")
