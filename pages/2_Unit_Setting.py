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

# 担当者一覧の取得
@st.cache_data(ttl=60)
def get_unique_tantousha():
    query = f"""
    SELECT DISTINCT `担当者`
    FROM `{project_id}.{dataset}.Final_Ad_Data`
    WHERE `担当者` IS NOT NULL AND `担当者` != ''
    ORDER BY `担当者`
    """
    return client.query(query).to_dataframe()

# Unit Mapping の取得
@st.cache_data(ttl=60)
def load_unit_mapping():
    query = f"SELECT * FROM `{full_table}`"
    return client.query(query).to_dataframe()

# 担当者追加UI
st.markdown("### ➕ 担当者をUnitに追加")

try:
    all_tantousha_df = get_unique_tantousha()
    current_df = load_unit_mapping()
    already_assigned = set(current_df["担当者"].dropna())
    unassigned_df = all_tantousha_df[~all_tantousha_df["担当者"].isin(already_assigned)]

    if unassigned_df.empty:
        st.info("✨ すべての担当者がUnitに振り分けられています。")
    else:
        selected_person = st.selectbox("👤 担当者を選択", unassigned_df["担当者"])
        input_unit = st.text_input("🏷️ 割り当てるUnit名")

        if st.button("＋ この担当者をUnitに追加"):
            if selected_person and input_unit:
                new_row = pd.DataFrame([{"担当者": selected_person, "Unit": input_unit}])
                updated_df = pd.concat([current_df, new_row], ignore_index=True)

                try:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("担当者", "STRING"),
                            bigquery.SchemaField("Unit", "STRING"),
                        ]
                    )
                    job = client.load_table_from_dataframe(updated_df, full_table, job_config=job_config)
                    job.result()
                    st.success(f"✅ {selected_person} を Unit『{input_unit}』に追加しました！")
                    st.cache_data.clear()
                    current_df = load_unit_mapping()  # 表を更新
                except Exception as e:
                    st.error(f"❌ 保存に失敗しました: {e}")
            else:
                st.warning("⚠️ Unit名を入力してください。")

except Exception as e:
    st.error(f"❌ 担当者一覧の取得に失敗しました: {e}")

# 編集可能なUnit割当一覧
st.markdown("---")
st.markdown("### 📝 既存のUnit割当を編集・並べ替え")

editable_df = st.data_editor(
    current_df.sort_values("担当者"),
    use_container_width=True,
    num_rows="dynamic",
    key="editable_unit_table"
)

if st.button("💾 編集内容を保存する"):
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("担当者", "STRING"),
                bigquery.SchemaField("Unit", "STRING"),
            ]
        )
        job = client.load_table_from_dataframe(editable_df, full_table, job_config=job_config)
        job.result()
        st.success("✅ 編集した内容を保存しました！")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"❌ 編集内容の保存に失敗しました: {e}")
