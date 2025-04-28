# 08_⚙️Client_Settings.py
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# --- ページ基本設定 ---
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定")

# --- BigQuery接続 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

PROJECT = "careful-chess-406412"
DATASET = "SHOSAN_Ad_Tokyo"
TABLE = "ClientSettings"
FULL_TABLE = f"{PROJECT}.{DATASET}.{TABLE}"

# --- テーブル読み込み ---
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{FULL_TABLE}`"
    return client.query(query).to_dataframe()

@st.cache_data(ttl=60)
def load_final_ad_data():
    query = f"SELECT DISTINCT client_name FROM `{PROJECT}.{DATASET}.Final_Ad_Data` WHERE client_name IS NOT NULL AND client_name != ''"
    return client.query(query).to_dataframe()

client_settings_df = load_client_settings()
final_ad_clients_df = load_final_ad_data()

# --- 既存登録のclient_nameをセットに ---
registered_clients = set(client_settings_df["client_name"].dropna())

# --- 新規登録できるクライアント一覧（未登録のもの） ---
unregistered_df = final_ad_clients_df[~final_ad_clients_df["client_name"].isin(registered_clients)]
unregistered_df = unregistered_df.sort_values("client_name")

st.markdown("### ➕ 新規クライアント登録")
if unregistered_df.empty:
    st.success("✅ すべてのクライアントが登録済みです！")
else:
    for idx, row in unregistered_df.iterrows():
        with st.expander(f"🆕 {row['client_name']}"):
            with st.form(f"new_client_form_{idx}"):
                client_id = st.text_input("固有ID (URL用英数字) ✏️", key=f"id_{idx}")
                building_count = st.text_input("棟数", key=f"building_{idx}")
                business_content = st.text_input("事業内容", key=f"business_{idx}")
                focus_level = st.text_input("注力度", key=f"focus_{idx}")
                submitted = st.form_submit_button("💾 保存")

                if submitted:
                    if not client_id:
                        st.warning("⚠️ 固有IDは必須です！")
                    else:
                        now = datetime.utcnow()
                        new_row = pd.DataFrame([{
                            "client_name": row["client_name"],
                            "client_id": client_id,
                            "building_count": building_count,
                            "business_content": business_content,
                            "focus_level": focus_level,
                            "created_at": now
                        }])

                        all_data = pd.concat([client_settings_df, new_row], ignore_index=True)

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
                        job = client.load_table_from_dataframe(all_data, FULL_TABLE, job_config=job_config)
                        job.result()
                        st.success("✅ 登録に成功しました！")
                        st.cache_data.clear()
                        st.experimental_rerun()

# --- 既存クライアント表示・編集 ---
st.markdown("### 📝 既存クライアント一覧")
editable_df = st.data_editor(
    client_settings_df.sort_values("client_name"),
    use_container_width=True,
    num_rows="dynamic",
    key="editable_client_table"
)

if st.button("💾 修正内容を保存"):
    try:
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
        job = client.load_table_from_dataframe(editable_df, FULL_TABLE, job_config=job_config)
        job.result()
        st.success("✅ 保存しました")
        st.cache_data.clear()
        st.experimental_rerun()
    except Exception as e:
        st.error(f"❌ 保存失敗: {e}")

# --- ページURL発行 ---
st.markdown("---")
st.markdown("### 🌐 クライアント別ページURLリンク発行")
base_url = st.secrets.get("base_app_url", "https://your-app-url")

for _, r in client_settings_df.iterrows():
    if r["client_id"]:
        link = f"{base_url}?client_id={r['client_id']}"
        st.markdown(f"[{r['client_name']}]({link}) ↗️", unsafe_allow_html=True)
