import streamlit as st
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="⚙️ Client Settings", layout="wide")
st.title("⚙️ Client Settings")
st.subheader("クライアント設定と個別ページ発行")

# BigQuery 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# テーブル情報
project_id = "careful-chess-406412"
dataset_id = "SHOSAN_Ad_Tokyo"
final_table = f"{project_id}.{dataset_id}.Final_Ad_Data"
settings_table = f"{project_id}.{dataset_id}.ClientSettings"

# クライアント一覧取得
@st.cache_data(ttl=60)
def load_client_names():
    query = f"""
    SELECT DISTINCT client_name
    FROM `{final_table}`
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# ClientSettingsテーブル取得
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{settings_table}`"
    return client.query(query).to_dataframe()

client_names_df = load_client_names()
try:
    client_settings_df = load_client_settings()
except:
    client_settings_df = pd.DataFrame(columns=["client_name", "client_id", "棟数", "事業内容", "注力度"])

registered_clients = set(client_settings_df["client_name"].dropna())

# 新規登録UI
st.markdown("### ➕ クライアントID 登録・更新")
for cname in client_names_df["client_name"]:
    st.markdown(f"#### {cname}")
    client_row = client_settings_df[client_settings_df["client_name"] == cname]

    input_id = st.text_input("クライアント固有ID", value=client_row["client_id"].values[0] if not client_row.empty else "", key=f"id_{cname}")
    input_house = st.text_input("棟数", value=client_row["棟数"].values[0] if not client_row.empty else "", key=f"house_{cname}")
    input_business = st.text_input("事業内容", value=client_row["事業内容"].values[0] if not client_row.empty else "", key=f"business_{cname}")
    input_focus = st.text_input("注力度", value=client_row["注力度"].values[0] if not client_row.empty else "", key=f"focus_{cname}")

    if st.button("💾 保存 / 更新", key=f"save_{cname}"):
        with st.spinner("保存中..."):
            new_row = pd.DataFrame([{
                "client_name": cname,
                "client_id": input_id,
                "棟数": input_house,
                "事業内容": input_business,
                "注力度": input_focus
            }])

            updated_df = client_settings_df.copy()
            updated_df = updated_df[updated_df["client_name"] != cname]
            updated_df = pd.concat([updated_df, new_row], ignore_index=True)

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField("client_name", "STRING"),
                    bigquery.SchemaField("client_id", "STRING"),
                    bigquery.SchemaField("棟数", "STRING"),
                    bigquery.SchemaField("事業内容", "STRING"),
                    bigquery.SchemaField("注力度", "STRING"),
                ]
            )
            job = client.load_table_from_dataframe(updated_df, settings_table, job_config=job_config)
            job.result()
            st.success(f"✅ {cname} の情報を保存しました！")
            st.cache_data.clear()

# --- 登録済み一覧 ---
st.markdown("---")
st.markdown("### 📋 登録済みクライアント")

if not client_settings_df.empty:
    for _, row in client_settings_df.iterrows():
        st.markdown(f"#### 🏢 {row['client_name']}")
        st.write(f"- 固有ID: `{row['client_id']}`")
        st.write(f"- 棟数: {row['棟数']}")
        st.write(f"- 事業内容: {row['事業内容']}")
        st.write(f"- 注力度: {row['注力度']}")

        # ページ発行リンク表示
        if row["client_id"]:
            base_url = st.secrets.get("base_url", "https://your-app-url")
            url = f"{base_url}/?client_id={row['client_id']}"
            st.markdown(f"[🌐 このクライアント専用ページを開く ↗️]({url})")

else:
    st.info("🔔 まだ登録されたクライアントはありません。")
