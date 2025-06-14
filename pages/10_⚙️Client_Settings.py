import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import secrets
import string

# --- ページ設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- テーブル情報 ---
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

def generate_random_suffix(length=30):
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(length))

clients_df = load_clients()
settings_df = load_client_settings()
registered_clients = set(settings_df["client_name"]) if not settings_df.empty else set()
unregistered_df = clients_df[~clients_df["client_name"].isin(registered_clients)]

# --- クライアント登録エリア ---
st.markdown("### ➕ 新しいクライアントを登録")

if unregistered_df.empty:
    st.info("✅ 登録可能な新規クライアントはありません")
else:
    with st.form("new_client_form"):
        selected_client = st.selectbox("👤 クライアント名を選択", unregistered_df["client_name"])
        input_id_prefix = st.text_input("🆔 クライアントIDの先頭 (英字のみ)", max_chars=20)
        building_count = st.text_input("🏠 棟数")
        business_content = st.text_input("💼 事業内容")
        focus_level = st.text_input("🚀 注力度")
        submitted = st.form_submit_button("＋ クライアントを登録")

        if submitted:
            if selected_client and input_id_prefix:
                random_suffix = generate_random_suffix()
                full_client_id = f"{input_id_prefix}_{random_suffix}"
                new_row = pd.DataFrame([{
                    "client_name": selected_client,
                    "client_id": full_client_id,
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
                st.warning("⚠️ クライアント名とIDの先頭を入力してください")

# --- 編集エリア ---
st.markdown("---")
st.markdown("### 📝 既存クライアント情報を編集")

if settings_df.empty:
    st.info("❗まだ登録されたクライアントはありません")
else:
    selected_edit_client = st.selectbox("✏️ 編集するクライアントを選択", settings_df["client_name"].unique())
    row = settings_df[settings_df["client_name"] == selected_edit_client].iloc[0]

    with st.form("edit_client_form"):
        updated_client_id = st.text_input("🆔 クライアントID", value=row["client_id"])
        updated_building_count = st.text_input("🏠 棟数", value=row["building_count"])
        updated_business_content = st.text_input("💼 事業内容", value=row["buisiness_content"])
        updated_focus_level = st.text_input("🚀 注力度", value=row["focus_level"])

        update_btn = st.form_submit_button("💾 編集内容を保存")

        if update_btn:
            settings_df.loc[settings_df["client_name"] == selected_edit_client, [
                "client_id", "building_count", "buisiness_content", "focus_level"
            ]] = [
                updated_client_id, updated_building_count, updated_business_content, updated_focus_level
            ]
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
                    job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                    job.result()
                    st.success("✅ 編集内容を保存しました！")
                    st.rerun()
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")

    with st.expander("🗑 このクライアント情報を削除"):
        confirm = st.radio("⚠️ 本当に削除しますか？", ("いいえ", "はい"), horizontal=True)
        if confirm == "はい":
            updated_df = settings_df[settings_df["client_name"] != selected_edit_client].copy()
            try:
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
                    job = client.load_table_from_dataframe(updated_df, full_table, job_config=job_config)
                    job.result()
                    st.success("🗑 クライアント情報を削除しました")
                    st.rerun()
            except Exception as e:
                st.error(f"❌ 削除エラー: {e}")
