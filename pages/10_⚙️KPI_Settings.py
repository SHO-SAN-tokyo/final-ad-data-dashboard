import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="KPI設定", layout="wide")
st.title("⚙️ KPI設定")

# BigQuery 認証
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# データ取得
@st.cache_data(ttl=60)
def load_target_data():
    return pd.read_gbq(f"SELECT * FROM `{project_id}.{table_id}`", project_id=project_id)

df = load_target_data()

# 編集フォーム
st.markdown("### 🎯 目標値の編集")
edited_df = st.data_editor(
    df,
    use_container_width=True,
    num_rows="dynamic",  # 新しい行の追加を許可
    column_config={
        "CPA目標": st.column_config.NumberColumn(format="¥%d"),
        "CVR目標": st.column_config.NumberColumn(format="%.2f %%"),
        "CTR目標": st.column_config.NumberColumn(format="%.2f %%"),
        "CPC目標": st.column_config.NumberColumn(format="¥%d"),
        "CPM目標": st.column_config.NumberColumn(format="¥%d"),
    }
)

# 保存ボタン
if st.button("💾 保存してBigQueryに反映"):
    try:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(info_dict)
        edited_df.to_gbq(
            destination_table=f"{table_id}",
            project_id=project_id,
            if_exists="replace",
            credentials=credentials
        )
        st.success("✅ BigQueryに保存しました！")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"❌ 保存に失敗しました: {e}")
