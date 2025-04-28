# 04_🛠️ClientSetting.py
import streamlit as st
import pandas as pd
from google.cloud import bigquery

# --- ページ設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("🛠️ クライアント設定ページ")
st.subheader("クライアントごとにIDなどを登録・編集・削除")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- Final_Ad_Dataからクライアント名一覧を取得 ---
@st.cache_data(show_spinner=False)
def load_clients():
    query = """
        SELECT DISTINCT クライアント名
        FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
        WHERE クライアント名 IS NOT NULL AND クライアント名 != ''
    """
    df = client.query(query).to_dataframe()
    return sorted(df["クライアント名"].unique())

clients = load_clients()

# --- クライアント管理テーブル ---
TABLE_ID = "careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSetting"

# --- 登録ボタン用関数 ---
def insert_client_setting(client_name, client_id, building_count, business_content, focus_level):
    table = client.get_table(TABLE_ID)
    rows = [{
        "client_name": client_name,
        "client_id": client_id,
        "building_count": building_count,
        "business_content": business_content,
        "focus_level": focus_level
    }]
    errors = client.insert_rows_json(table, rows)
    return errors

# --- 削除ボタン用関数 ---
def delete_client_setting(client_name):
    query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE client_name = @client_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("client_name", "STRING", client_name)
        ]
    )
    client.query(query, job_config=job_config).result()

# --- クライアント登録エリア ---
st.markdown("<h5>🆕 クライアント新規登録</h5>", unsafe_allow_html=True)

for cname in clients:
    with st.expander(f"➕ {cname}"):
        client_id = st.text_input(f"{cname} の Client ID", key=f"id_{cname}")
        building_count = st.text_input(f"{cname} の棟数", key=f"building_{cname}")
        business_content = st.text_input(f"{cname} の事業内容", key=f"business_{cname}")
        focus_level = st.text_input(f"{cname} の注力度", key=f"focus_{cname}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("登録する", key=f"register_{cname}"):
                errors = insert_client_setting(cname, client_id, building_count, business_content, focus_level)
                if not errors:
                    st.success(f"{cname} を登録しました！")
                else:
                    st.error(f"登録エラー: {errors}")
        with col2:
            if st.button("登録解除 (削除)", key=f"delete_{cname}"):
                delete_client_setting(cname)
                st.warning(f"{cname} を削除しました")

# --- 現在登録されているクライアント一覧表示 ---
st.markdown("<h5>📋 現在登録されているクライアント</h5>", unsafe_allow_html=True)
latest = client.query(f"SELECT * FROM `{TABLE_ID}` ORDER BY client_name").to_dataframe()
if latest.empty:
    st.info("現在登録されているクライアントがありません")
else:
    st.dataframe(latest, use_container_width=True)
