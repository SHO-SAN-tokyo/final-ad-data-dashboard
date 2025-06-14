import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import random
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

# --- ランダムなclient_id生成 ---
def generate_client_id(prefix: str) -> str:
    rand_str = ''.join(random.choices(string.ascii_letters + string.digits, k=30))
    return f"{prefix}_{rand_str}"

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
    auto_generated_id = generate_client_id(selected_client)
    input_id = st.text_input("🆔 クライアント固有IDを入力", value=auto_generated_id, key="register_id")
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

# --- 既存登録クライアント一覧（確認のみ） ---
st.markdown("---")
st.markdown("### 📋 登録済みクライアント一覧（確認用）")

if settings_df.empty:
    st.info("❗まだ登録されたクライアントはありません")
else:
    st.dataframe(settings_df.sort_values("client_name"), use_container_width=True)

# --- 編集エリア ---
st.markdown("---")
st.markdown("### 🛠 クライアント情報の編集")

if not settings_df.empty:
    edit_names = settings_df["client_name"].sort_values().tolist()
    selected_edit_client = st.selectbox("✏️ 編集するクライアントを選択", edit_names)
    row = settings_df[settings_df["client_name"] == selected_edit_client].iloc[0]

    # 入力欄
    new_client_id = st.text_input("🆔 クライアントID", value=row["client_id"], key="edit_client_id")
    if st.button("🔄 ランダム再生成", key="regenerate_id_btn"):
        regenerated_id = generate_client_id(selected_edit_client)
        st.session_state["edit_client_id"] = regenerated_id
        new_client_id = regenerated_id

    updated_building_count = st.text_input("🏠 棟数", value=row["building_count"], key="edit_building_count")
    updated_business_content = st.text_input("💼 事業内容", value=row["buisiness_content"], key="edit_business_content")
    updated_focus_level = st.text_input("🚀 注力度", value=row["focus_level"], key="edit_focus_level")

    if st.button("💾 編集内容を保存"):
        settings_df.loc[settings_df["client_name"] == selected_edit_client, [
            "client_id", "building_count", "buisiness_content", "focus_level"
        ]] = [new_client_id, updated_building_count, updated_business_content, updated_focus_level]

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
                st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ 保存エラー: {e}")

# --- クライアント別リンク一覧 ---
st.markdown("---")
st.markdown("### 🔗 クライアント別ページリンク")

if not settings_df.empty:
    link_df = settings_df[["client_name", "building_count", "buisiness_content", "focus_level", "client_id"]].copy()
    link_df["リンクURL"] = link_df["client_id"].apply(
        lambda cid: f"https://{st.secrets['app_domain']}/Ad_Drive?client_id={cid}"
    )

    for _, row in link_df.iterrows():
        cols = st.columns([2, 1, 2, 1, 2])
        cols[0].write(row["client_name"])
        cols[1].write(row["building_count"])
        cols[2].write(row["buisiness_content"])
        cols[3].write(row["focus_level"])
        cols[4].markdown(
            f"""
            <a href=\"{row['リンクURL']}\" target=\"_blank\" style=\"
                text-decoration: none;
                display: inline-block;
                padding: 0.3em 0.8em;
                border-radius: 6px;
                background-color: #4CAF50;
                color: white;
                font-weight: bold;
            \">
                ▶ ページを開く
            </a>
            """,
            unsafe_allow_html=True
        )
