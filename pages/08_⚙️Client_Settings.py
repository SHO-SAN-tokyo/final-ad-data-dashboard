import streamlit as st
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定ページ")
st.subheader("クライアント情報の管理")

# BigQuery 接続
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# テーブル設定
table_name = "careful-chess-406412.SHOSAN_Ad_Tokyo.ClientSettings"

# Final_Ad_Dataからクライアント名取得
@st.cache_data(show_spinner=False)
def load_client_names():
    df = client.query("""
        SELECT DISTINCT client_name
        FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data`
        WHERE client_name IS NOT NULL AND client_name != ""
    """).to_dataframe()
    return sorted(df["client_name"].dropna().unique().tolist())

# 登録済みクライアント一覧取得
@st.cache_data(show_spinner=False)
def load_registered_clients():
    df = client.query(f"""
        SELECT * FROM `{table_name}`
    """).to_dataframe()
    return df

client_names = load_client_names()
registered_clients = load_registered_clients()

# --- クライアント一覧表示と登録エリア ---
st.markdown("<h4>📋 登録・編集エリア</h4>", unsafe_allow_html=True)

for name in client_names:
    with st.expander(f"{name}", expanded=False):
        # 既存データを探す
        existing = registered_clients[registered_clients["クライアント名"] == name]
        default_id = existing["クライアントID"].values[0] if not existing.empty else ""
        default_building = existing["棟数"].values[0] if not existing.empty else ""
        default_business = existing["事業内容"].values[0] if not existing.empty else ""
        default_focus = existing["注力度"].values[0] if not existing.empty else ""

        # 入力フォーム
        client_id = st.text_input(f"クライアントID ({name})", value=default_id, key=f"id_{name}")
        building = st.text_input("棟数", value=default_building, key=f"building_{name}")
        business = st.text_input("事業内容", value=default_business, key=f"business_{name}")
        focus = st.text_input("注力度", value=default_focus, key=f"focus_{name}")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 保存・更新", key=f"save_{name}"):
                # INSERTまたはUPDATE
                query = f"""
                    MERGE `{table_name}` AS T
                    USING (SELECT @クライアント名 AS クライアント名) AS S
                    ON T.クライアント名 = S.クライアント名
                    WHEN MATCHED THEN
                      UPDATE SET クライアントID = @クライアントID, 棟数 = @棟数, 事業内容 = @事業内容, 注力度 = @注力度
                    WHEN NOT MATCHED THEN
                      INSERT (クライアント名, クライアントID, 棟数, 事業内容, 注力度)
                      VALUES(@クライアント名, @クライアントID, @棟数, @事業内容, @注力度)
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("クライアント名", "STRING", name),
                        bigquery.ScalarQueryParameter("クライアントID", "STRING", client_id),
                        bigquery.ScalarQueryParameter("棟数", "STRING", building),
                        bigquery.ScalarQueryParameter("事業内容", "STRING", business),
                        bigquery.ScalarQueryParameter("注力度", "STRING", focus),
                    ]
                )
                client.query(query, job_config=job_config).result()
                st.success(f"{name} を保存・更新しました。ページを再読み込みしてください。")

        with c2:
            if not existing.empty:
                if st.button("🗑️ 削除", key=f"delete_{name}"):
                    del_query = f"""
                        DELETE FROM `{table_name}`
                        WHERE クライアント名 = @クライアント名
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("クライアント名", "STRING", name)
                        ]
                    )
                    client.query(del_query, job_config=job_config).result()
                    st.success(f"{name} を削除しました。ページを再読み込みしてください。")

st.caption("※ 保存・更新・削除後はページを手動でリロードしてください 🚀")
