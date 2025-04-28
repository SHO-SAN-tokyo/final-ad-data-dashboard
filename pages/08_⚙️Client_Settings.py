import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# ページ設定
st.set_page_config(page_title="Client Settings", layout="wide")
st.title("🔧 Client Settings (クライアント管理)")

# --- BigQuery 証明 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- 定数セットアップ ---
PROJECT_ID = "careful-chess-406412"
DATASET = "SHOSAN_Ad_Tokyo"
TABLE = "ClientSettings"
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# --- クライアント一覧 (存在分)を取得 ---
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{FULL_TABLE}`"
    return client.query(query).to_dataframe()

# --- Final_Ad_Dataからクライアント名を取得 ---
@st.cache_data(ttl=60)
def get_unique_clients():
    query = f"""
        SELECT DISTINCT client_name
        FROM `{PROJECT_ID}.{DATASET}.Final_Ad_Data`
        WHERE client_name IS NOT NULL AND client_name != ''
        ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# --- データ取得 ---
try:
    client_df = get_unique_clients()
    current_df = load_client_settings()
    registered_clients = set(current_df["client_name"].dropna())
except Exception as e:
    st.error(f"データ取得失敗: {e}")
    st.stop()

# --- 新規登録 ---
st.markdown("### ➕ 新しいクライアントを登録")

unregistered = client_df[~client_df["client_name"].isin(registered_clients)]
if unregistered.empty:
    st.info("すべてのクライアントが登録済みです。")
else:
    selected_client = st.selectbox("💼 クライアント名", unregistered["client_name"])
    input_client_id = st.text_input("👤 クライアントID")
    input_building_count = st.text_input("🛏️ 棟数")
    input_business_content = st.text_input("💼 事業内容")
    input_focus_level = st.text_input("🔍 注力度")

    if st.button("➕ 登録"): 
        if selected_client and input_client_id:
            new_row = pd.DataFrame([{
                "client_name": selected_client,
                "client_id": input_client_id,
                "building_count": input_building_count,
                "business_content": input_business_content,
                "focus_level": input_focus_level,
                "created_at": datetime.now()
            }])
            updated_df = pd.concat([current_df, new_row], ignore_index=True)
            try:
                with st.spinner("保存中..."):
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("client_name", "STRING"),
                            bigquery.SchemaField("client_id", "STRING"),
                            bigquery.SchemaField("building_count", "STRING"),
                            bigquery.SchemaField("business_content", "STRING"),
                            bigquery.SchemaField("focus_level", "STRING"),
                            bigquery.SchemaField("created_at", "TIMESTAMP"),
                        ]
                    )
                    job = client.load_table_from_dataframe(updated_df, FULL_TABLE, job_config=job_config)
                    job.result()
                    st.success(f"✅ {selected_client} を登録しました！")
                    st.cache_data.clear()
                    current_df = load_client_settings()
            except Exception as e:
                st.error(f"保存失敗: {e}")
        else:
            st.warning("クライアントIDを入力してください")

# --- リンク一覧 ---
st.markdown("---")
st.markdown("### 🔗 登録されたクライアントのリンク")

if current_df.empty:
    st.info("登録済みクライアントはありません")
else:
    for _, r in current_df.sort_values("client_name").iterrows():
        st.markdown(f"""
        #### 👉 {r['client_name']}
        [🔗 ID: {r['client_id']} のページを開く](https://YOUR-STREAMLIT-APP-URL/?client_id={r['client_id']})
        """
        )

# --- CSS ---
st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #4a84da;
        color: white;
        border: 1px solid #4a84da;
        border-radius: 0.5rem;
        padding: 0.6em 1.2em;
        font-weight: 600;
        transition: 0.3s ease;
    }
    div.stButton > button:first-child:hover {
        background-color: #3f77cc;
        border-color: #3f77cc;
    }
    </style>
""", unsafe_allow_html=True)
