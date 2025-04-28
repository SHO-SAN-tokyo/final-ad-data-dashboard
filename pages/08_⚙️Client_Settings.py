import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="Client設定", layout="wide")
st.title("⚙️ Client Settings")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- テーブル情報 ---
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "ClientSettings"
full_table = f"{project_id}.{dataset}.{table}"

st.write(st.secrets)

# --- クライアント一覧取得 ---
@st.cache_data(ttl=60)
def load_clients():
    query = f"SELECT DISTINCT client_name FROM `{project_id}.{dataset}.Final_Ad_Data` WHERE client_name IS NOT NULL AND client_name != '' ORDER BY client_name"
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
    input_id = st.text_input("🆔 クライアント固有IDを入力")
    building_count = st.text_input("🏠 棟数")
    business_content = st.text_input("💼 事業内容")
    focus_level = st.text_input("🚀 注力度")

    if st.button("＋ クライアントを登録"):
        if selected_client and input_id:
            new_row = pd.DataFrame([{
                "client_name": selected_client,
                "client_id": input_id,
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
            st.warning("⚠️ クライアントIDを入力してください")

# --- 既存登録クライアント一覧・編集エリア ---
st.markdown("---")
st.markdown("### 📝 既存クライアント一覧（編集可）")

if settings_df.empty:
    st.info("❗まだ登録されたクライアントはありません")
else:
    editable_df = st.data_editor(
        settings_df.sort_values("client_name"),
        num_rows="dynamic",
        use_container_width=True,
        key="editable_client_table"
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

# --- クライアント別リンク一覧 ---
st.markdown("---")
st.markdown("### 🔗 クライアント別ページリンク")

if settings_df.empty:
    st.info("❗登録されたクライアントがありません")
else:
    link_df = settings_df[["client_name", "client_id"]].copy()
    link_df["リンク"] = link_df["client_id"].apply(
        lambda cid: f"[ページを開く](https://{st.secrets['app_domain']}/Ad_Drive?client_id={cid})"
    )
    st.dataframe(link_df, use_container_width=True)
