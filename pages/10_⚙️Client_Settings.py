import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import random
import string

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()


# ──────────────────────────────────────────────
# クライアント設定
# ──────────────────────────────────────────────
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
    FROM {project_id}.{dataset}.Final_Ad_Data
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# --- 登録済み設定取得 ---
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM {full_table}"
    return client.query(query).to_dataframe()

def generate_random_suffix(length=30):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

clients_df = load_clients()
settings_df = load_client_settings()

# --- 未登録クライアント取得 ---
registered_clients = set(settings_df["client_name"]) if not settings_df.empty else set()
unregistered_df = clients_df[~clients_df["client_name"].isin(registered_clients)]

# --- 新規登録 ---
st.markdown("### ➕ 新しいクライアントを登録")
if unregistered_df.empty:
    st.info("✅ 登録可能な新規クライアントはありません")
else:
    selected_client = st.selectbox("👤 クライアント名を選択", unregistered_df["client_name"])
    client_id_prefix = st.text_input("🆔 クライアントIDを入力 (クライアントID完成例: livebest_ランダム文字列)")

    if "random_suffix" not in st.session_state:
        st.session_state["random_suffix"] = generate_random_suffix()

    st.markdown("📋 下のランダム文字列をコピーして、クライアントIDの末尾に貼り付ける：")
    st.code(f"_{st.session_state['random_suffix']}", language="plaintext")

    building_count = st.text_input("🏠 棟数")
    business_content = st.text_input("💼 事業内容")
    focus_level = st.text_input("🚀 注力度")

    if st.button("＋ クライアントを登録"):
        if selected_client and client_id_prefix:
            client_id = f"{client_id_prefix}_{st.session_state['random_suffix']}"
            new_row = pd.DataFrame([{
                "client_name": selected_client,
                "client_id": client_id,
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
                    del st.session_state["random_suffix"]
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")
        else:
            st.warning("⚠️ クライアントIDを入力してください")


# --- クライアント情報の編集 ---
st.markdown("---")
st.markdown("### 📝 既存クライアントの編集")

if settings_df.empty:
    st.info("❗まだ登録されたクライアントはありません")
else:
    client_names = settings_df["client_name"].unique().tolist()
    selected_name = st.selectbox("👤 編集するクライアントを選択", ["--- 選択してください ---"] + client_names)

    if selected_name != "--- 選択してください ---":
        row = settings_df[settings_df["client_name"] == selected_name].iloc[0]

        with st.form("edit_form"):
            updated_client_id = st.text_input("🆔 クライアントID", value=row["client_id"])
            updated_building_count = st.text_input("🏠 棟数", value=row["building_count"])
            updated_business_content = st.text_input("💼 事業内容", value=row["buisiness_content"])
            updated_focus_level = st.text_input("🚀 注力度", value=row["focus_level"])
            submitted = st.form_submit_button("💾 保存")

            if submitted:
                try:
                    settings_df.loc[settings_df["client_name"] == selected_name, [
                        "client_id", "building_count", "buisiness_content", "focus_level"
                    ]] = [
                        updated_client_id,
                        updated_building_count,
                        updated_business_content,
                        updated_focus_level
                    ]
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
                        st.success("✅ 保存が完了しました！")
                        st.cache_data.clear()
                        settings_df = load_client_settings()
                except Exception as e:
                    st.error(f"❌ 保存エラー: {e}")

        with st.expander("🗑 このクライアント情報を削除"):
            if st.button("❌ クライアントを削除"):
                try:
                    settings_df = settings_df[settings_df["client_name"] != selected_name]
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
                        st.success("🗑 削除が完了しました")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ 削除エラー: {e}")

# --- クライアント別リンク一覧 ---
st.markdown("---")
st.markdown("### 🔗 クライアント別ページリンク（一覧表示）")

if settings_df.empty:
    st.info("❗登録されたクライアントがありません")
else:
    link_df = settings_df[["client_name", "building_count", "buisiness_content", "focus_level", "client_id"]].copy()
    link_df["リンクURL"] = link_df["client_id"].apply(
        lambda cid: f"https://sho-san-client-ad-score.streamlit.app/?client_id={cid}"
    )

    st.divider()

    # ヘッダー
    header_cols = st.columns([2, 2, 1, 1.5, 1.5])
    header_cols[0].markdown("**クライアント名**")
    header_cols[1].markdown("**リンク**")
    header_cols[2].markdown("**注力度**")
    header_cols[3].markdown("**事業内容**")
    header_cols[4].markdown("**棟数**")

    st.divider()

    def vertical_center(content, height="70px"):
        safe_content = content if pd.notna(content) and str(content).strip() != "" else "&nbsp;"
        return f"""
        <div style="display: flex; align-items: center; height: {height}; min-height: {height};">
            {safe_content}
        </div>
        """

    for _, row in link_df.iterrows():
        cols = st.columns([2, 2, 1, 1.5, 1.5])

        row_height = "70px"
        row_style = f"border-bottom: 1px solid #ddd; height: {row_height}; min-height: {row_height}; display: flex; align-items: center;"

        with cols[0]:
            st.markdown(f'<div style="{row_style}">{row["client_name"]}</div>', unsafe_allow_html=True)

        with cols[1]:
            button_html = f"""
            <a href="{row['リンクURL']}" target="_blank" style="
                text-decoration: none;
                display: inline-block;
                padding: 0.3em 0.8em;
                border-radius: 6px;
                background-color: rgb(53, 169, 195);
                color: white;
                font-weight: bold;">
                ▶ ページを開く
            </a>
            """
            st.markdown(f'<div style="{row_style}">{button_html}</div>', unsafe_allow_html=True)

        with cols[2]:
            st.markdown(f'<div style="{row_style}">{row["focus_level"] or "&nbsp;"}</div>', unsafe_allow_html=True)

        with cols[3]:
            st.markdown(f'<div style="{row_style}">{row["buisiness_content"] or "&nbsp;"}</div>', unsafe_allow_html=True)

        with cols[4]:
            st.markdown(f'<div style="{row_style}">{row["building_count"] or "&nbsp;"}</div>', unsafe_allow_html=True)




