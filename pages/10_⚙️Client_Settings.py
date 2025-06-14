import streamlit as st
import pandas as pd
import random
import string
from google.cloud import bigquery
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\n", "\n")
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

# --- クライアント登録エリア ---
st.markdown("### ➕ 新しいクライアントを登録")

if unregistered_df.empty:
    st.info("✅ 登録可能な新規クライアントはありません")
else:
    selected_client = st.selectbox("👤 クライアント名を選択", unregistered_df["client_name"])
    prefix_input = st.text_input("🆔 クライアントIDのプレフィックスを入力 (例: livebest)")
    building_count = st.text_input("🏠 棟数")
    business_content = st.text_input("💼 事業内容")
    focus_level = st.text_input("🚀 注力度")

    if st.button("＋ クライアントを登録"):
        if selected_client and prefix_input:
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=30))
            generated_id = f"{prefix_input}_{random_suffix}"

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
            st.warning("⚠️ プレフィックスを入力してください")

# --- 登録済みクライアント一覧（確認用） ---
st.markdown("---")
st.markdown("### 📋 登録済みクライアント一覧（確認用）")

if settings_df.empty:
    st.info("❗まだ登録されたクライアントはありません")
else:
    st.dataframe(settings_df.sort_values("client_name"), use_container_width=True)

# --- 編集エリア（KPI Settings風） ---
st.markdown("---")
st.markdown("### 🛠 クライアント情報の編集（KPI Settings風）")

if not settings_df.empty:
    selected_name = st.selectbox("編集するクライアント名を選択", options=settings_df["client_name"].unique())
    edit_index = settings_df[settings_df["client_name"] == selected_name].index[0]
    row = settings_df.loc[edit_index]

    st.markdown(f"#### 📝 このクライアントを編集・削除：{selected_name}")

    updated_client_name = st.text_input("👤 クライアント名", value=row["client_name"], key="name")
    updated_client_id = st.text_input("🆔 クライアントID", value=row["client_id"], key="cid")
    updated_building_count = st.text_input("🏠 棟数", value=row["building_count"], key="building")
    updated_business_content = st.text_input("💼 事業内容", value=row["buisiness_content"], key="biz")
    updated_focus_level = st.text_input("🚀 注力度", value=row["focus_level"], key="focus")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("この内容で上書き保存", key="save"):
            settings_df.at[edit_index, "client_name"] = updated_client_name
            settings_df.at[edit_index, "client_id"] = updated_client_id
            settings_df.at[edit_index, "building_count"] = updated_building_count
            settings_df.at[edit_index, "buisiness_content"] = updated_business_content
            settings_df.at[edit_index, "focus_level"] = updated_focus_level

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
                    st.success("✅ 上書き保存が完了しました")
                    st.cache_data.clear()
                    settings_df = load_client_settings()
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")

    with col2:
        if st.button("この行を削除する", key="delete"):
            settings_df = settings_df.drop(index=edit_index).reset_index(drop=True)
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
                    job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                    job.result()
                    st.success("✅ 削除が完了しました")
                    st.cache_data.clear()
                    settings_df = load_client_settings()
            except Exception as e:
                st.error(f"❌ 削除エラー: {e}")
