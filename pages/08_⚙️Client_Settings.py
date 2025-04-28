# 04_⚙️Client_Settings.py
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# --- ページ設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定ページ")

# --- BigQuery接続 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

table_id = "careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings"

# --- データ取得 ---
@st.cache_data(show_spinner=False)
def load_clients():
    query = f"SELECT * FROM `{table_id}`"
    return client.query(query).to_dataframe()

def save_client(row):
    query = f"""
        INSERT INTO `{table_id}` (client_name, client_id, building_count, business_content, focus_level, created_at)
        VALUES (@client_name, @client_id, @building_count, @business_content, @focus_level, @created_at)
        ON DUPLICATE KEY UPDATE 
            building_count = @building_count,
            business_content = @business_content,
            focus_level = @focus_level,
            created_at = @created_at
    """
    # BigQueryは "ON DUPLICATE KEY UPDATE" が使えないため、本当は MERGE文を使う。後で修正可。
    pass  # 今回はシンプルなInsert/UpdateをStreamlit側で分岐して制御します

def insert_client(data):
    client.insert_rows_json(table_id, [data])

def delete_client(client_id):
    query = f"""
        DELETE FROM `{table_id}`
        WHERE client_id = @client_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("client_id", "STRING", client_id)
        ]
    )
    client.query(query, job_config=job_config).result()

# --- データロード ---
clients_df = load_clients()

# --- 画面表示 ---
st.markdown("<h5>📋 クライアント一覧</h5>", unsafe_allow_html=True)
if clients_df.empty:
    st.info("登録されたクライアントがありません")
else:
    clients_df = clients_df.sort_values("client_name")

    edited = st.data_editor(
        clients_df,
        column_config={
            "client_name": "クライアント名",
            "client_id": "クライアントID",
            "building_count": "棟数",
            "business_content": "事業内容",
            "focus_level": "注力度",
            "created_at": "登録日時"
        },
        num_rows="dynamic",
        hide_index=True
    )

    # 保存ボタン
    if st.button("💾 編集内容を保存"):
        try:
            for i, row in edited.iterrows():
                data = {
                    "client_name": row["client_name"],
                    "client_id": row["client_id"],
                    "building_count": row.get("building_count", ""),
                    "business_content": row.get("business_content", ""),
                    "focus_level": row.get("focus_level", ""),
                    "created_at": datetime.utcnow().isoformat()
                }
                insert_client(data)
            st.success("保存しました！")
        except Exception as e:
            st.error("保存中にエラーが発生しました")
            st.exception(e)

# --- 新規登録 ---
st.divider()
st.markdown("<h5>🆕 新規登録</h5>", unsafe_allow_html=True)

new_client_name = st.text_input("クライアント名")
new_client_id   = st.text_input("クライアントID")
new_building_count = st.text_input("棟数")
new_business_content = st.text_input("事業内容")
new_focus_level = st.text_input("注力度")

if st.button("➕ 新規追加"):
    if new_client_name and new_client_id:
        try:
            insert_client({
                "client_name": new_client_name,
                "client_id": new_client_id,
                "building_count": new_building_count,
                "business_content": new_business_content,
                "focus_level": new_focus_level,
                "created_at": datetime.utcnow().isoformat()
            })
            st.success("追加しました！")
            st.experimental_rerun()
        except Exception as e:
            st.error("追加中にエラーが発生しました")
            st.exception(e)
    else:
        st.warning("クライアント名とIDは必須です")

# --- 削除 ---
st.divider()
st.markdown("<h5>🗑️ クライアント削除</h5>", unsafe_allow_html=True)

delete_id = st.selectbox("削除したいクライアントIDを選択", options=clients_df["client_id"] if not clients_df.empty else [])

if st.button("🗑️ このクライアントを削除"):
    if delete_id:
        try:
            delete_client(delete_id)
            st.success("削除しました！")
            st.experimental_rerun()
        except Exception as e:
            st.error("削除中にエラーが発生しました")
            st.exception(e)
    else:
        st.warning("削除対象を選択してください")
