import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 認証 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(info_dict)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

project_id = "careful-chess-406412"
source_table = "SHOSAN_Ad_Tokyo.Final_Ad_Data_Last"
target_table = "SHOSAN_Ad_Tokyo.Target_Indicators_Meta"

st.set_page_config(page_title="⚙️ KPI設定", layout="wide")
st.title("⚙️ 広告KPI設定")

# --- 候補値取得 ---
@st.cache_data(ttl=60)
def get_unique_values():
    query = f"""
        SELECT DISTINCT `広告媒体`, `メインカテゴリ`, `サブカテゴリ`, `広告目的`
        FROM {project_id}.{source_table}
        WHERE `広告媒体` IS NOT NULL
          AND `メインカテゴリ` IS NOT NULL
          AND `サブカテゴリ` IS NOT NULL
          AND `広告目的` IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    return (
        sorted(df["広告媒体"].dropna().unique()),
        sorted(df["メインカテゴリ"].dropna().unique()),
        sorted(df["サブカテゴリ"].dropna().unique()),
        sorted(df["広告目的"].dropna().unique())
    )

広告媒体一覧, メインカテゴリ一覧, サブカテゴリ一覧, 広告目的一覧 = get_unique_values()

# --- 既存データ取得 ---
@st.cache_data(ttl=60)
def load_target_data():
    try:
        query = f"SELECT * FROM `{project_id}`.`{target_table}`"
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame(columns=[
            "広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的",
            "CPA_best", "CPA_good", "CPA_min",
            "CVR_best", "CVR_good", "CVR_min",
            "CTR_best", "CTR_good", "CTR_min",
            "CPC_best", "CPC_good", "CPC_min",
            "CPM_best", "CPM_good", "CPM_min"
        ])

target_df = load_target_data()

st.markdown("### 🎯 新しいKPIを追加")
with st.form("add_kpi_form"):
    col1, col2, col3, col4 = st.columns(4)
    ad_media = col1.selectbox("広告媒体", options=広告媒体一覧)
    main_cat = col2.selectbox("メインカテゴリ", options=メインカテゴリ一覧)
    sub_cat = col3.selectbox("サブカテゴリ", options=サブカテゴリ一覧)
    obj = col4.selectbox("広告目的", options=広告目的一覧)

    st.write("#### 指標値を入力（任意）")
    cpa_best = st.number_input("CPA_best", min_value=0.0, step=1.0, format="%.0f", value=0.0)
    cpa_good = st.number_input("CPA_good", min_value=0.0, step=1.0, format="%.0f", value=0.0)
    cpa_min = st.number_input("CPA_min", min_value=0.0, step=1.0, format="%.0f", value=0.0)
    # 必要に応じてCVR/CTR/CPC/CPMなども追加
    submitted = st.form_submit_button("追加")

    if submitted:
        # すでに同じ組み合わせがあれば警告
        is_dup = ((target_df["広告媒体"] == ad_media) &
                  (target_df["メインカテゴリ"] == main_cat) &
                  (target_df["サブカテゴリ"] == sub_cat) &
                  (target_df["広告目的"] == obj)).any()
        if is_dup:
            st.warning("⚠️ この組み合わせは既に登録されています。")
        else:
            new_row = pd.DataFrame([{
                "広告媒体": ad_media,
                "メインカテゴリ": main_cat,
                "サブカテゴリ": sub_cat,
                "広告目的": obj,
                "CPA_best": cpa_best if cpa_best > 0 else None,
                "CPA_good": cpa_good if cpa_good > 0 else None,
                "CPA_min": cpa_min if cpa_min > 0 else None,
                # ... 他指標もここに
            }])
            target_df = pd.concat([target_df, new_row], ignore_index=True)
            st.success("✅ 新規KPIを追加しました。※この時点では保存されません")

st.markdown("### 👀 現在のKPI設定（編集・削除）")
edited_df = st.data_editor(
    target_df,
    use_container_width=True,
    num_rows="dynamic",
    hide_index=True,
    key="edit_kpi"
)

# --- 保存 ---
if st.button("💾 保存する"):
    with st.spinner("保存中..."):
        try:
            save_columns = [
                "広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的",
                "CPA_best", "CPA_good", "CPA_min",
                "CVR_best", "CVR_good", "CVR_min",
                "CTR_best", "CTR_good", "CTR_min",
                "CPC_best", "CPC_good", "CPC_min",
                "CPM_best", "CPM_good", "CPM_min"
            ]
            save_df = edited_df[save_columns]
            save_df.to_gbq(
                destination_table=target_table,
                project_id=project_id,
                if_exists="replace",
                credentials=credentials
            )
            st.success("✅ データの保存に成功しました！")
            st.cache_data.clear()
        except Exception as e:
            st.error("❌ 保存に失敗しました。エラー内容を確認してください。")
            st.exception(e)

# --- CSS調整は省略（ご要望あれば追記します） ---
