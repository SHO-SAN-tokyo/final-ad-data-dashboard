import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# ページ設定
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ Client設定")
st.markdown("クライアントIDを設定・管理するページです")

# BigQuery 認証
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# テーブル情報
PROJECT_ID = "careful-chess-406412"
DATASET = "SHOSAN_Ad_Tokyo"
TABLE = "ClientSettings"
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# クライアント一覧（Final_Ad_Dataから取得）
@st.cache_data(ttl=300)
def load_client_list():
    query = f"""
    SELECT DISTINCT client_name
    FROM `{PROJECT_ID}.{DATASET}.Final_Ad_Data`
    WHERE client_name IS NOT NULL
      AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# 登録済み設定（ClientSettingsから取得）
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{FULL_TABLE}`"
    return client.query(query).to_dataframe()

# データ取得
client_list_df = load_client_list()
client_settings_df = load_client_settings()

# 登録済みクライアント名リスト
registered_clients = set(client_settings_df["client_name"]) if not client_settings_df.empty else set()

# --- 新規登録エリア ---
st.markdown("### ➕ 新規クライアントIDを登録")
unregistered_df = client_list_df[~client_list_df["client_name"].isin(registered_clients)]

if unregistered_df.empty:
    st.info("✨ 登録可能なクライアントがありません。")
else:
    sel_client = st.selectbox("クライアント名を選択", unregistered_df["client_name"])
    input_id = st.text_input("クライアント固有IDを入力")
    input_building = st.text_input("棟数を入力")
    input_business = st.text_input("事業内容を入力")
    input_focus = st.text_input("注力度を入力")

    if st.button("＋ 登録する"):
        if sel_client and input_id:
            new_row = pd.DataFrame([{
                "client_name": sel_client,
                "client_id": input_id,
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
                    job = client.load_table_from_dataframe(updated_df, FULL_TABLE, job_config=job_config)
                    job.result()
                    st.success("✅ 登録が完了しました！")
                    st.cache_data.clear()
                    client_settings_df = load_client_settings()
            except Exception as e:
                st.error(f"❌ 登録に失敗しました: {e}")
        else:
            st.warning("⚠️ クライアントIDを入力してください。")

# --- 登録済み一覧エリア ---
st.markdown("---")
st.markdown("### 📋 登録済みクライアント一覧とリンク発行")

if client_settings_df.empty:
    st.info("登録されているクライアントがありません。")
else:
    client_settings_df = client_settings_df.sort_values("client_name")

    client_settings_df["リンク"] = client_settings_df.apply(
        lambda r: f"[🔗リンクを開く](https://shosan-ad-dashboard.streamlit.app/Ad_Drive?client_id={r['client_id']})",
        axis=1
    )

    st.dataframe(
        client_settings_df[["client_name", "client_id", "building_count", "buisiness_content", "focus_level", "created_at", "リンク"]],
        use_container_width=True
    )

# --- ボタンCSSカスタマイズ ---
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
