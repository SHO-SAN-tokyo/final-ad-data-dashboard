# 08_⚙️Client_Settings.py
import streamlit as st
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="Client設定", layout="wide")
st.title("⚙️ Client設定")

# BigQuery 認証
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# テーブル情報
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "ClientSettings"
full_table = f"{project_id}.{dataset}.{table}"

# クライアント一覧の取得
@st.cache_data(ttl=60)
def get_unique_clients():
    query = f"""
    SELECT DISTINCT client_name
    FROM `{project_id}.{dataset}.Final_Ad_Data`
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# Client Settings テーブル取得
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{full_table}`"
    return client.query(query).to_dataframe()

# クライアント追加 UI
st.markdown("### ➕ クライアントを登録/更新")

try:
    all_clients_df = get_unique_clients()
    current_df = load_client_settings()
    already_registered = set(current_df["client_name"].dropna())
    unregistered_df = all_clients_df[~all_clients_df["client_name"].isin(already_registered)]

    col1, col2 = st.columns([2, 3])
    with col1:
        st.markdown("#### 📋 既存クライアント一覧")
        st.dataframe(all_clients_df, use_container_width=True)

    with col2:
        st.markdown("#### 🆕 新規登録エリア")
        selected_client = st.selectbox("🆔 クライアント名を選択", unregistered_df["client_name"])
        input_client_id = st.text_input("🔑 クライアントID (URLパラメータに使う)")
        input_buildings = st.text_input("🏘️ 棟数")
        input_business = st.text_input("💼 事業内容")
        input_focus = st.text_input("🎯 注力度")

        if st.button("＋ このクライアントを登録"):
            if selected_client and input_client_id:
                new_row = pd.DataFrame([{
                    "client_name": selected_client,
                    "client_id": input_client_id,
                    "buildings": input_buildings,
                    "business": input_business,
                    "focus": input_focus
                }])
                updated_df = pd.concat([current_df, new_row], ignore_index=True)

                try:
                    with st.spinner("保存中です..."):
                        job_config = bigquery.LoadJobConfig(
                            write_disposition="WRITE_TRUNCATE",
                            schema=[
                                bigquery.SchemaField("client_name", "STRING"),
                                bigquery.SchemaField("client_id", "STRING"),
                                bigquery.SchemaField("buildings", "STRING"),
                                bigquery.SchemaField("business", "STRING"),
                                bigquery.SchemaField("focus", "STRING")
                            ]
                        )
                        job = client.load_table_from_dataframe(updated_df, full_table, job_config=job_config)
                        job.result()
                        st.success(f"✅ {selected_client} を登録しました！")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ 保存に失敗しました: {e}")
            else:
                st.warning("⚠️ クライアントIDを入力してください。")

except Exception as e:
    st.error(f"❌ クライアント一覧の取得に失敗しました: {e}")

# 編集可能なClient Settings一覧
st.markdown("---")
st.markdown("### 📝 登録済みクライアント設定を編集")

editable_df = st.data_editor(
    load_client_settings().sort_values("client_name"),
    use_container_width=True,
    num_rows="dynamic",
    key="editable_client_table"
)

if st.button("💾 編集内容を保存する"):
    with st.spinner("保存中です..."):
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField("client_name", "STRING"),
                    bigquery.SchemaField("client_id", "STRING"),
                    bigquery.SchemaField("buildings", "STRING"),
                    bigquery.SchemaField("business", "STRING"),
                    bigquery.SchemaField("focus", "STRING")
                ]
            )
            job = client.load_table_from_dataframe(editable_df, full_table, job_config=job_config)
            job.result()
            st.success("✅ 編集した内容を保存しました！")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ 保存に失敗しました: {e}")

# --- ボタンの色をカスタマイズ（CSS適用） ---
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
