import streamlit as st
import pandas as pd
from google.cloud import bigquery

# --- ページ設定 ---
st.set_page_config(page_title="Client設定", layout="wide")
st.title("⚙️ Client設定")

# --- BigQuery 接続 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

project = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
client_table = "ClientSettings"
full_client_table = f"{project}.{dataset}.{client_table}"

# --- データ取得関数 ---
@st.cache_data(ttl=60)
def load_clients():
    query = f"SELECT * FROM `{full_client_table}`"
    return client.query(query).to_dataframe()

@st.cache_data(ttl=60)
def load_clientnames():
    query = f"""
        SELECT DISTINCT `client_name`
        FROM `{project}.{dataset}.Final_Ad_Data`
        WHERE `client_name` IS NOT NULL AND `client_name` != ''
        ORDER BY `client_name`
    """
    return client.query(query).to_dataframe()

# --- データ読み込み ---
client_settings_df = load_clients()
all_clients_df = load_clientnames()

registered_clients = set(client_settings_df["クライアント名"]) if not client_settings_df.empty else set()

# --- 新規登録エリア ---
st.markdown("### ➕ クライアント情報を登録")

available_clients = all_clients_df[~all_clients_df["client_name"].isin(registered_clients)]

if available_clients.empty:
    st.success("✅ すべてのクライアントが登録されています。")
else:
    with st.form("register_client"):
        selected_client = st.selectbox("🆕 クライアントを選択", available_clients["client_name"], index=0)
        input_id = st.text_input("🆔 クライアントID (例: abc123)")
        input_tousu = st.text_input("🏠 棟数 (例: 10棟)")
        input_biz = st.text_input("🏢 事業内容 (例: 分譲、注文住宅)")
        input_focus = st.text_input("🎯 注力度 (例: 高、低、中)")
        submitted = st.form_submit_button("＋ 登録する")

    if submitted:
        if selected_client and input_id:
            new_entry = pd.DataFrame([{ 
                "クライアント名": selected_client, 
                "クライアントID": input_id,
                "棟数": input_tousu,
                "事業内容": input_biz,
                "注力度": input_focus
            }])
            updated_df = pd.concat([client_settings_df, new_entry], ignore_index=True)

            try:
                with st.spinner("保存中..."):
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("クライアント名", "STRING"),
                            bigquery.SchemaField("クライアントID", "STRING"),
                            bigquery.SchemaField("棟数", "STRING"),
                            bigquery.SchemaField("事業内容", "STRING"),
                            bigquery.SchemaField("注力度", "STRING"),
                        ]
                    )
                    job = client.load_table_from_dataframe(updated_df, full_client_table, job_config=job_config)
                    job.result()
                    st.success(f"✅ {selected_client} を登録しました！")
                    st.cache_data.clear()
                    st.experimental_rerun()
            except Exception as e:
                st.error(f"❌ 保存に失敗しました: {e}")
        else:
            st.warning("⚠️ IDを必ず入力してください。")

# --- 登録済みクライアント一覧 ---
st.markdown("---")
st.markdown("### 📋 登録済みクライアント一覧")

if client_settings_df.empty:
    st.info("🈳 登録済みクライアントがありません。")
else:
    editable_df = st.data_editor(
        client_settings_df,
        use_container_width=True,
        num_rows="dynamic",
        key="client_settings_table"
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("💾 修正内容を保存する"):
            with st.spinner("保存中..."):
                try:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("クライアント名", "STRING"),
                            bigquery.SchemaField("クライアントID", "STRING"),
                            bigquery.SchemaField("棟数", "STRING"),
                            bigquery.SchemaField("事業内容", "STRING"),
                            bigquery.SchemaField("注力度", "STRING"),
                        ]
                    )
                    job = client.load_table_from_dataframe(editable_df, full_client_table, job_config=job_config)
                    job.result()
                    st.success("✅ 保存しました！")
                    st.cache_data.clear()
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"❌ 保存失敗: {e}")

    with col2:
        if st.button("🗑️ 全件削除（注意！）"):
            with st.spinner("全削除中..."):
                try:
                    empty_df = pd.DataFrame(columns=client_settings_df.columns)
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("クライアント名", "STRING"),
                            bigquery.SchemaField("クライアントID", "STRING"),
                            bigquery.SchemaField("棟数", "STRING"),
                            bigquery.SchemaField("事業内容", "STRING"),
                            bigquery.SchemaField("注力度", "STRING"),
                        ]
                    )
                    job = client.load_table_from_dataframe(empty_df, full_client_table, job_config=job_config)
                    job.result()
                    st.success("🗑️ 全削除しました！")
                    st.cache_data.clear()
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"❌ 全削除失敗: {e}")

# --- URLリンク生成エリア ---
st.markdown("---")
st.markdown("### 🔗 クライアント別リンク発行")

if not client_settings_df.empty:
    for _, row in client_settings_df.iterrows():
        client_name = row["クライアント名"]
        client_id   = row["クライアントID"]
        page_url = f"https://{st.secrets['app_domain']}/1_Main_Dashboard?client_id={client_id}"

        with st.container():
            st.markdown(f"""
                <div style='padding:12px;margin-bottom:10px;border:1px solid #ccc;border-radius:8px;'>
                  <b>🏢 クライアント名：</b> {client_name}<br>
                  <b>🔗 フィルタ済みURL：</b> <a href='{page_url}' target='_blank'>{page_url}</a>
                </div>
            """, unsafe_allow_html=True)
