import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ページ設定
st.set_page_config(page_title="⚙️ KPI設定", layout="wide")
st.title("⚙️ 広告KPI設定")

# --- 認証 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(info_dict)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# --- 定数 ---
project_id = "careful-chess-406412"
source_table = "SHOSAN_Ad_Tokyo.Final_Ad_Data"
target_table = "SHOSAN_Ad_Tokyo.Target_Indicators"

# --- 広告データからカテゴリと広告目的を取得 ---
@st.cache_data(ttl=60)
def get_unique_values():
    try:
        query = f"""
            SELECT DISTINCT `カテゴリ`, `広告目的`
            FROM `{project_id}.{source_table}`
            WHERE `カテゴリ` IS NOT NULL AND `広告目的` IS NOT NULL
        """
        df = client.query(query).to_dataframe()
        return df["カテゴリ"].unique(), df["広告目的"].unique()
    except Exception as e:
        st.error(f"❌ カテゴリ・広告目的の取得に失敗しました: {e}")
        st.stop()

カテゴリ一覧, 広告目的一覧 = get_unique_values()

# --- 既存の目標データ読み込み ---
@st.cache_data(ttl=60)
def load_target_data():
    try:
        query = f"SELECT * FROM `{project_id}.{target_table}`"
        return client.query(query).to_dataframe()
    except Exception as e:
        st.warning("⚠️ まだ目標データが存在しない可能性があります。")
        return pd.DataFrame(columns=[
            "カテゴリ", "広告目的", "CPA目標", "CVR目標", "CTR目標", "CPC目標", "CPM目標"
        ])

target_df = load_target_data()

# --- 編集用の空白行追加 ---
for cat in カテゴリ一覧:
    for obj in 広告目的一覧:
        if not ((target_df["カテゴリ"] == cat) & (target_df["広告目的"] == obj)).any():
            target_df = pd.concat([
                target_df,
                pd.DataFrame([{
                    "カテゴリ": cat,
                    "広告目的": obj,
                    "CPA目標": None,
                    "CVR目標": None,
                    "CTR目標": None,
                    "CPC目標": None,
                    "CPM目標": None
                }])
            ], ignore_index=True)

# --- 編集UI ---
st.markdown("### 🎯 カテゴリ × 広告目的ごとの目標値を設定")
edited_df = st.data_editor(
    target_df.sort_values(["カテゴリ", "広告目的"]),
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "CPA目標": st.column_config.NumberColumn(format="¥%d"),
        "CVR目標": st.column_config.NumberColumn(format="%.2f %%"),
        "CTR目標": st.column_config.NumberColumn(format="%.2f %%"),
        "CPC目標": st.column_config.NumberColumn(format="¥%d"),
        "CPM目標": st.column_config.NumberColumn(format="¥%d"),
    }
)

# --- 保存状態管理 ---
if "saving" not in st.session_state:
    st.session_state["saving"] = False

# --- 保存用フォーム ---
with st.form("save_form"):
    disabled = st.session_state["saving"]
    button_label = "保存中..." if disabled else "💾 保存する"

    submitted = st.form_submit_button(
        button_label,
        disabled=disabled,
        use_container_width=True
    )

    if submitted:
        st.session_state["saving"] = True
        try:
            save_df = edited_df[["カテゴリ", "広告目的", "CPA目標", "CVR目標", "CTR目標", "CPC目標", "CPM目標"]]
            save_df.to_gbq(
                destination_table=target_table,
                project_id=project_id,
                if_exists="replace",
                credentials=credentials
            )
            st.success("✅ 保存しました！")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ 保存に失敗しました: {e}")
        finally:
            st.session_state["saving"] = False

# --- カスタムボタンスタイル（保存中グレー対応） ---
st.markdown("""
    <style>
    div.stButton > button {
        background-color: #4a84da;
        color: white;
        border: 1px solid #4a84da;
        border-radius: 0.5rem;
        padding: 0.6em 1.2em;
        font-weight: 600;
        transition: 0.3s ease;
    }
    div.stButton > button:hover:enabled {
        background-color: #3f77cc;
        border-color: #3f77cc;
    }
    div.stButton > button:disabled {
        background-color: #cccccc !important;
        color: #666666 !important;
        border-color: #bbbbbb !important;
        cursor: not-allowed;
    }
    </style>
""", unsafe_allow_html=True)
