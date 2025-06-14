import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import secrets
import string

# --- クライアントID自動生成関数 ---
def generate_client_id(prefix: str):
    charset = string.ascii_lowercase + string.digits
    random_part = ''.join(secrets.choice(charset) for _ in range(30))
    return f"{prefix}_{random_part}"

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

clients_df = load_clients()
settings_df = load_client_settings()

# --- 未登録クライアント取得 ---
registered_clients = set(settings_df["client_name"]) if not settings_df.empty else set()
unregistered_df = clients_df[~clients_df["client_name"].isin(registered_clients)]

# --- 新規登録 ---
st.markdown("### ➕ 新しいクライアントを登録")

if unregistered_df.empty:
    st.info("✅ 登録可能な新規クライアントはありません")
else:
    selected_client = st.selectbox("👤 クライアント名", unregistered_df["client_name"])
    prefix = st.text_input("🆔 クライアントIDの先頭 (英字のみ)", value="livebest")

    generated_id = generate_client_id(prefix.strip()) if prefix else ""
    if prefix:
        st.text_input("🔒 実際に保存されるclient_id", value=generated_id, disabled=True)

    building_count = st.text_input("🏠 棟数")
    business_content = st.text_input("💼 事業内容")
    focus_level = st.text_input("🚀 注力度")

    if st.button("＋ クライアントを登録"):
        if selected_client and prefix:
            new_row = pd.DataFrame([{
                "client_name": selected_client,
                "client_id": generated_id,
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
                    st.cache_data.clear()
                    settings_df = load_client_settings()
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")
        else:
            st.warning("⚠️ IDの先頭を入力してください")

# --- 登録済みクライアント確認テーブル ---
st.markdown("---")
st.markdown("### 🧾 登録済クライアント一覧（確認用）")

if settings_df.empty:
    st.info("❓ まだ登録されたクライアントはありません")
else:
    st.dataframe(settings_df, use_container_width=True)

# --- 編集機能付きエリア ---
st.markdown("---")
st.markdown("### 🛠 KPI Settings（編集＆保存）")

if settings_df.empty:
    st.info("❗ 編集できるクライアントデータがありません")
else:
    editable_df = st.data_editor(
        settings_df.sort_values("client_name").reset_index(drop=True),
        num_rows="dynamic",
        use_container_width=True,
        key="kpi_settings_editor"
    )

    if st.button("💾 編集内容を保存"):
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
                job = client.load_table_from_dataframe(editable_df, full_table, job_config=job_config)
                job.result()
                st.success("✅ 編集内容を保存しました！")
                st.cache_data.clear()
                settings_df = load_client_settings()
        except Exception as e:
            st.error(f"❌ 保存エラー: {e}")
