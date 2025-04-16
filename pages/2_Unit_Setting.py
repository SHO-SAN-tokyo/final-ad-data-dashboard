# 2_Unit_Setting.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="Unit設定", layout="wide")
st.title("⚙️ Unit設定ページ")

# BigQuery 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# テーブル情報
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "UnitMapping"
full_table = f"{project_id}.{dataset}.{table}"

# 初期データ取得
@st.cache_data(ttl=60)
def load_unit_mapping():
    query = f"SELECT * FROM `{full_table}`"
    return client.query(query).to_dataframe()

df = load_unit_mapping()

# 編集 UI
st.markdown("### 担当者とUnitの対応表")
edited_df = st.data_editor(
    df,
    num_rows="dynamic",
    use_container_width=True,
    key="unit_mapping_editor"
)

# 保存処理
if st.button("💾 保存してBigQueryに反映"):
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # 全削除して上書き
            schema=[
                bigquery.SchemaField("担当者", "STRING"),
                bigquery.SchemaField("Unit", "STRING"),
            ]
        )
        job = client.load_table_from_dataframe(edited_df, full_table, job_config=job_config)
        job.result()
        st.success("✅ 保存しました！（BigQueryに反映されました）")
        st.cache_data.clear()  # キャッシュをクリアして再取得できるように
    except Exception as e:
        st.error(f"❌ 保存に失敗しました: {e}")


