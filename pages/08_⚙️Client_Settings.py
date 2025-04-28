import streamlit as st
import pandas as pd
from google.cloud import bigquery

# --- ページ設定 ---
st.set_page_config(page_title="Client設定", layout="wide")
st.title("⚙️ Client設定")

# --- BigQuery接続 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

project_id = "careful-chess-406412"
dataset_id = "SHOSAN_Ad_Tokyo"
final_ad_table = f"{project_id}.{dataset_id}.Final_Ad_Data"
client_settings_table = f"{project_id}.{dataset_id}.ClientSettings"

# --- クライアント名一覧を取得 ---
@st.cache_data(ttl=300)
def load_client_names():
    query = f"""
    SELECT DISTINCT client_name
    FROM `{final_ad_table}`
    WHERE client_name IS NOT NULL
    ORDER BY client_name
    """
    df = client.query(query).to_dataframe()
    return df["client_name"].dropna().tolist()

# --- ClientSettingsテーブルを取得 ---
@st.cache_data(ttl=300)
def load_client_settings():
    query = f"SELECT * FROM `{client_settings_table}`"
    df = client.query(query).to_dataframe()
    return df

# --- 初期データ読み込み ---
client_names = load_client_names()
try:
    client_settings_df = load_client_settings()
except:
    client_settings_df = pd.DataFrame(columns=["クライアント名", "ID", "棟数", "事業内容", "注力度"])

registered_clients = set(client_settings_df["クライアント名"]) if not client_settings_df.empty else set()
unregistered_clients = sorted(list(set(client_names) - registered_clients))

# --- 新規登録エリア ---
st.markdown("### ➕ 新しいクライアントIDを登録する")

if unregistered_clients:
    selected_client = st.selectbox("クライアント名を選択", unregistered_clients)
    col1, col2, col3 = st.columns(3)
    with col1:
        new_id = st.text_input("🆔 クライアント専用ID（英数字推奨）")
    with col2:
        new_tousu = st.text_input("🏠 棟数")
    with col3:
        new_biz = st.text_input("💼 事業内容")
    new_focus = st.text_input("✨ 注力度（例：高、中、低）")

    if st.button("✅ 登録する"):
        if selected_client and new_id:
            with st.spinner("登録中..."):
                updated_df = pd.concat([
                    client_settings_df,
                    pd.DataFrame([{
                        "クライアント名": selected_client,
                        "ID": new_id,
                        "棟数": new_tousu,
                        "事業内容": new_biz,
                        "注力度": new_focus
                    }])
                ], ignore_index=True)

                # 保存
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    schema=[
                        bigquery.SchemaField("クライアント名", "STRING"),
                        bigquery.SchemaField("ID", "STRING"),
                        bigquery.SchemaField("棟数", "STRING"),
                        bigquery.SchemaField("事業内容", "STRING"),
                        bigquery.SchemaField("注力度", "STRING"),
                    ]
                )
                job = client.load_table_from_dataframe(updated_df, client_settings_table, job_config=job_config)
                job.result()
                st.success(f"✅ {selected_client} を登録しました！")
                st.cache_data.clear()
                st.experimental_rerun()
        else:
            st.warning("⚠️ IDを入力してください。")
else:
    st.info("✨ 全クライアントが登録済みです")

# --- 既存登録クライアント一覧（ボタンリンク付き） ---
st.markdown("---")
st.markdown("### 🔗 登録済みクライアント一覧 & 専用ページボタン")

if not client_settings_df.empty:
    base_url = "https://xxx-your-app-url-xxx/1_Main_Dashboard"  # ← あなたの本番URLに変更してね！

    for idx, row in client_settings_df.iterrows():
        cols = st.columns([2, 2, 2, 2, 2, 1])

        with cols[0]:
            st.markdown(f"**{row['クライアント名']}**")
        with cols[1]:
            st.markdown(f"{row['ID']}")
        with cols[2]:
            st.markdown(f"{row['棟数']}")
        with cols[3]:
            st.markdown(f"{row['事業内容']}")
        with cols[4]:
            st.markdown(f"{row['注力度']}")
        with cols[5]:
            if pd.notna(row['ID']):
                link = f"{base_url}?client_id={row['ID']}"
                st.markdown(f"""
                    <a href="{link}" target="_blank">
                        <button style="
                            background-color:#4CAF50;
                            border:none;
                            color:white;
                            padding:6px 10px;
                            text-align:center;
                            text-decoration:none;
                            display:inline-block;
                            font-size:14px;
                            border-radius:6px;
                            cursor:pointer;">
                        ▶️ 開く
                        </button>
                    </a>
                """, unsafe_allow_html=True)
            else:
                st.markdown("-")
else:
    st.info("まだ登録されたクライアントがありません")

# --- ボタンのデザイン統一 ---
st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #4a84da;
        color: white;
        border: 1px solid #4a84da;
        border-radius: 0.5rem;
        padding: 0.6em 1.2em;
        font-weight: 600;
        transition: 0.3s ease;
    }
    div.stButton > button:first-child:hover {
        background-color: #3f77cc;
        border-color: #3f77cc;
    }
    </style>
""", unsafe_allow_html=True)
