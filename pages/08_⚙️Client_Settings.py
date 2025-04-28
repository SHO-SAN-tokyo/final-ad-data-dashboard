import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery

# --- ページ設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定ページ")
st.subheader("クライアントフィルタ用のID管理・編集")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- データ取得 ---
@st.cache_data(show_spinner=False)
def load_clients():
    query = """
        SELECT DISTINCT client_name
        FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`
        WHERE client_name IS NOT NULL AND client_name != ""
        ORDER BY client_name
    """
    return client.query(query).to_dataframe()

@st.cache_data(show_spinner=False)
def load_settings():
    query = """
        SELECT *
        FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings`
        ORDER BY client_name
    """
    return client.query(query).to_dataframe()

clients_df = load_clients()
settings_df = load_settings()

# --- 登録・編集エリア ---
st.markdown("### 📝 新規登録・編集")

for _, row in clients_df.iterrows():
    client_name = row["client_name"]

    existing = settings_df[settings_df["client_name"] == client_name]
    if not existing.empty:
        default_id = existing.iloc[0]["client_id"]
        default_building = existing.iloc[0]["building_count"]
        default_business = existing.iloc[0]["business_type"]
        default_focus = existing.iloc[0]["focus_level"]
    else:
        default_id = ""
        default_building = ""
        default_business = ""
        default_focus = ""

    with st.expander(f"{client_name}", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            new_id = st.text_input(f"固有ID（{client_name}）", value=default_id, key=f"id_{client_name}")
            building = st.text_input(f"棟数（{client_name}）", value=default_building, key=f"building_{client_name}")
        with col2:
            business = st.text_input(f"事業内容（{client_name}）", value=default_business, key=f"business_{client_name}")
            focus = st.text_input(f"注力度（{client_name}）", value=default_focus, key=f"focus_{client_name}")

        save_btn = st.button("💾 保存/更新", key=f"save_{client_name}")
        delete_btn = st.button("🗑️ 削除", key=f"delete_{client_name}")

        if save_btn:
            query = f"""
                MERGE `careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings` T
                USING (SELECT '{client_name}' AS client_name) S
                ON T.client_name = S.client_name
                WHEN MATCHED THEN
                  UPDATE SET client_id = '{new_id}', building_count = '{building}', business_type = '{business}', focus_level = '{focus}'
                WHEN NOT MATCHED THEN
                  INSERT (client_name, client_id, building_count, business_type, focus_level)
                  VALUES ('{client_name}', '{new_id}', '{building}', '{business}', '{focus}')
            """
            client.query(query).result()
            st.success(f"{client_name} を保存しました！")
            st.rerun()

        if delete_btn:
            query = f"""
                DELETE FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings`
                WHERE client_name = '{client_name}'
            """
            client.query(query).result()
            st.success(f"{client_name} を削除しました！")
            st.rerun()

# --- 既存データ一覧 ---
st.markdown("### 📋 登録済みクライアント一覧")
st.dataframe(settings_df, use_container_width=True)
