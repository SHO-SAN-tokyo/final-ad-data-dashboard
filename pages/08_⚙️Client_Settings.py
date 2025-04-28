# 08_⚙️Client_Settings.py  クライアント設定ページ（最新設計）

import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery

# --- 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定")

# --- データ取得 ---
@st.cache_data(show_spinner=False)
def load_client_list():
    query = """
    SELECT DISTINCT client_name
    FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
    WHERE client_name IS NOT NULL
    ORDER BY client_name
    """
    df = client.query(query).to_dataframe()
    return df

@st.cache_data(show_spinner=False)
def load_client_settings():
    query = """
    SELECT *
    FROM careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings
    """
    df = client.query(query).to_dataframe()
    return df

client_list_df = load_client_list()
client_settings_df = load_client_settings()

# --- 新規登録セクション ---
st.markdown("<h4>🆕 新規クライアント設定登録</h4>", unsafe_allow_html=True)

unregistered_clients = sorted(set(client_list_df["client_name"]) - set(client_settings_df["client_name"]))

if not unregistered_clients:
    st.success("すべてのクライアントが登録済みです ✨")
else:
    with st.form("new_client_form"):
        selected_client = st.selectbox("クライアント名を選択", unregistered_clients)
        client_id = st.text_input("固有ID (URLパラメータ用)")
        house_count = st.text_input("棟数 (任意)")
        business_type = st.text_input("事業内容 (任意)")
        priority = st.text_input("注力度 (任意)")
        submitted = st.form_submit_button("登録する")

        if submitted:
            if not client_id:
                st.error("固有IDは必須です")
            else:
                insert_query = f"""
                INSERT INTO careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings
                (client_name, client_id, 棟数, 事業内容, 注力度)
                VALUES
                ('{selected_client}', '{client_id}', '{house_count}', '{business_type}', '{priority}')
                """
                client.query(insert_query)
                st.success(f"✅ {selected_client} を登録しました！")
                st.cache_data.clear()
                st.experimental_rerun()

# --- 登録済みリスト＆編集・削除セクション ---
st.markdown("<h4>📋 登録済みクライアント一覧</h4>", unsafe_allow_html=True)

if client_settings_df.empty:
    st.warning("登録されたクライアントがありません")
else:
    for idx, row in client_settings_df.sort_values("client_name").iterrows():
        with st.expander(f"{row['client_name']} ({row['client_id']})", expanded=False):
            c1, c2, c3, c4, c5 = st.columns([2,2,2,2,1])
            with c1:
                new_id = st.text_input("固有ID", value=row["client_id"], key=f"id_{idx}")
            with c2:
                new_house = st.text_input("棟数", value=row.get("棟数", ""), key=f"house_{idx}")
            with c3:
                new_biz = st.text_input("事業内容", value=row.get("事業内容", ""), key=f"biz_{idx}")
            with c4:
                new_priority = st.text_input("注力度", value=row.get("注力度", ""), key=f"prio_{idx}")
            with c5:
                if st.button("💾 更新", key=f"update_{idx}"):
                    update_query = f"""
                    UPDATE careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings
                    SET client_id = '{new_id}', 棟数 = '{new_house}', 事業内容 = '{new_biz}', 注力度 = '{new_priority}'
                    WHERE client_name = '{row['client_name']}'
                    """
                    client.query(update_query)
                    st.success(f"{row['client_name']} を更新しました！")
                    st.cache_data.clear()
                    st.experimental_rerun()
            if st.button("🗑️ 削除", key=f"delete_{idx}"):
                delete_query = f"""
                DELETE FROM careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings
                WHERE client_name = '{row['client_name']}'
                """
                client.query(delete_query)
                st.warning(f"{row['client_name']} を削除しました")
                st.cache_data.clear()
                st.experimental_rerun()
