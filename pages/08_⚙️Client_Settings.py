import streamlit as st
import pandas as pd
from google.cloud import bigquery
import urllib.parse

st.set_page_config(page_title="⚙️ Client Settings", layout="wide")
st.title("⚙️ Client Settings")
st.subheader("クライアントごとの設定管理")

# BigQuery認証
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# テーブル情報
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
client_table = "ClientSettings"
full_client_table = f"{project_id}.{dataset}.{client_table}"

# クライアント名をFinal_Ad_Dataから取得
@st.cache_data(ttl=60)
def load_final_clients():
    query = f"""
    SELECT DISTINCT client_name
    FROM `{project_id}.{dataset}.Final_Ad_Data`
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# ClientSettingsのデータを取得
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM `{full_client_table}`"
    return client.query(query).to_dataframe()

final_clients_df = load_final_clients()
client_settings_df = load_client_settings()

# 未登録クライアント一覧
registered_clients = set(client_settings_df["クライアント名"].dropna())
unregistered_df = final_clients_df[~final_clients_df["client_name"].isin(registered_clients)]

# 新規登録エリア
st.markdown("---")
st.markdown("### ➕ 新規クライアント登録")

for _, row in unregistered_df.iterrows():
    with st.expander(f"{row['client_name']}"):
        new_id = st.text_input(f"クライアントID（{row['client_name']}）", key=f"new_id_{row['client_name']}")
        new_units = st.text_input(f"棟数（{row['client_name']}）", key=f"new_units_{row['client_name']}")
        new_business = st.text_input(f"事業内容（{row['client_name']}）", key=f"new_business_{row['client_name']}")
        new_focus = st.text_input(f"注力度（{row['client_name']}）", key=f"new_focus_{row['client_name']}")
        
        if st.button(f"保存（{row['client_name']}）", key=f"save_{row['client_name']}"):
            try:
                new_row = pd.DataFrame([{ 
                    "クライアント名": row['client_name'],
                    "クライアントID": new_id,
                    "棟数": new_units,
                    "事業内容": new_business,
                    "注力度": new_focus
                }])
                updated_df = pd.concat([client_settings_df, new_row], ignore_index=True)
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    schema=[
                        bigquery.SchemaField("クライアント名", "STRING"),
                        bigquery.SchemaField("クライアントID", "STRING"),
                        bigquery.SchemaField("棟数", "STRING"),
                        bigquery.SchemaField("事業内容", "STRING"),
                        bigquery.SchemaField("注力度", "STRING")
                    ]
                )
                client.load_table_from_dataframe(updated_df, full_client_table, job_config=job_config).result()
                st.success(f"✅ {row['client_name']} を登録しました！")
                st.cache_data.clear()
                st.experimental_rerun()
            except Exception as e:
                st.error(f"❌ 登録失敗: {e}")

# 登録済みクライアント一覧
st.markdown("---")
st.markdown("### 📝 登録済みクライアント一覧")

if client_settings_df.empty:
    st.info("登録されたクライアントがありません。")
else:
    for idx, row in client_settings_df.sort_values("クライアント名").iterrows():
        client_name = row["クライアント名"]
        client_id = row["クライアントID"]
        base_url = st.secrets["app"]["base_url"]
        page_url = f"{base_url}?client_id={urllib.parse.quote(client_id)}"
        
        col1, col2, col3 = st.columns([4, 1, 1])
        with col1:
            st.markdown(f"**{client_name}**")
        with col2:
            st.link_button("ページ発行", page_url)
        with col3:
            if st.button(f"🗑️ 削除", key=f"delete_{idx}"):
                try:
                    new_df = client_settings_df.drop(idx).reset_index(drop=True)
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("クライアント名", "STRING"),
                            bigquery.SchemaField("クライアントID", "STRING"),
                            bigquery.SchemaField("棟数", "STRING"),
                            bigquery.SchemaField("事業内容", "STRING"),
                            bigquery.SchemaField("注力度", "STRING")
                        ]
                    )
                    client.load_table_from_dataframe(new_df, full_client_table, job_config=job_config).result()
                    st.success(f"✅ {client_name} を削除しました！")
                    st.cache_data.clear()
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"❌ 削除失敗: {e}")
