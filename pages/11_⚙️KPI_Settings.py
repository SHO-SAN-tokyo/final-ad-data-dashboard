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

# --- セッション状態でデータを保持 ---
if "target_df" not in st.session_state:
    st.session_state.target_df = load_target_data()

target_df = st.session_state.target_df

st.markdown("### 🎯 新しいKPIを追加")
with st.form("add_kpi_form", clear_on_submit=True):
    col1, col2, col3, col4 = st.columns(4)
    ad_media = col1.selectbox("広告媒体", options=広告媒体一覧, key="add_admedia")
    main_cat = col2.selectbox("メインカテゴリ", options=メインカテゴリ一覧, key="add_maincat")
    sub_cat = col3.selectbox("サブカテゴリ", options=サブカテゴリ一覧, key="add_subcat")
    obj = col4.selectbox("広告目的", options=広告目的一覧, key="add_obj")

    st.markdown("#### 指標値をすべて入力")
    cols = st.columns(9)
    cpa_best = cols[0].number_input("CPA_best", min_value=0.0, step=1.0, format="%.0f", key="add_cpa_best")
    cpa_good = cols[1].number_input("CPA_good", min_value=0.0, step=1.0, format="%.0f", key="add_cpa_good")
    cpa_min = cols[2].number_input("CPA_min", min_value=0.0, step=1.0, format="%.0f", key="add_cpa_min")
    cvr_best = cols[3].number_input("CVR_best", min_value=0.0, step=0.01, format="%.2f", key="add_cvr_best")
    cvr_good = cols[4].number_input("CVR_good", min_value=0.0, step=0.01, format="%.2f", key="add_cvr_good")
    cvr_min = cols[5].number_input("CVR_min", min_value=0.0, step=0.01, format="%.2f", key="add_cvr_min")
    ctr_best = cols[6].number_input("CTR_best", min_value=0.0, step=0.01, format="%.2f", key="add_ctr_best")
    ctr_good = cols[7].number_input("CTR_good", min_value=0.0, step=0.01, format="%.2f", key="add_ctr_good")
    ctr_min = cols[8].number_input("CTR_min", min_value=0.0, step=0.01, format="%.2f", key="add_ctr_min")
    cols2 = st.columns(9)
    cpc_best = cols2[0].number_input("CPC_best", min_value=0.0, step=1.0, format="%.0f", key="add_cpc_best")
    cpc_good = cols2[1].number_input("CPC_good", min_value=0.0, step=1.0, format="%.0f", key="add_cpc_good")
    cpc_min = cols2[2].number_input("CPC_min", min_value=0.0, step=1.0, format="%.0f", key="add_cpc_min")
    cpm_best = cols2[3].number_input("CPM_best", min_value=0.0, step=1.0, format="%.0f", key="add_cpm_best")
    cpm_good = cols2[4].number_input("CPM_good", min_value=0.0, step=1.0, format="%.0f", key="add_cpm_good")
    cpm_min = cols2[5].number_input("CPM_min", min_value=0.0, step=1.0, format="%.0f", key="add_cpm_min")

    submitted = st.form_submit_button("追加")
    if submitted:
        # 重複チェック
        is_dup = (
            (target_df["広告媒体"] == ad_media) &
            (target_df["メインカテゴリ"] == main_cat) &
            (target_df["サブカテゴリ"] == sub_cat) &
            (target_df["広告目的"] == obj)
        ).any()
        if is_dup:
            st.warning("⚠️ この組み合わせは既に登録されています。")
        else:
            new_row = pd.DataFrame([{
                "広告媒体": ad_media,
                "メインカテゴリ": main_cat,
                "サブカテゴリ": sub_cat,
                "広告目的": obj,
                "CPA_best": cpa_best, "CPA_good": cpa_good, "CPA_min": cpa_min,
                "CVR_best": cvr_best, "CVR_good": cvr_good, "CVR_min": cvr_min,
                "CTR_best": ctr_best, "CTR_good": ctr_good, "CTR_min": ctr_min,
                "CPC_best": cpc_best, "CPC_good": cpc_good, "CPC_min": cpc_min,
                "CPM_best": cpm_best, "CPM_good": cpm_good, "CPM_min": cpm_min,
            }])
            st.session_state.target_df = pd.concat([target_df, new_row], ignore_index=True)
            st.success("✅ 新しいKPIを追加しました（※保存は下のボタンで）")
            st.experimental_rerun()

st.markdown("### 👀 KPI設定一覧")

# --- 行選択 ---
if len(target_df) > 0:
    selected_index = st.selectbox(
        "編集・削除したい行を選択",
        options=target_df.index,
        format_func=lambda i: f"{target_df.loc[i, '広告媒体']} | {target_df.loc[i, 'メインカテゴリ']} | {target_df.loc[i, 'サブカテゴリ']} | {target_df.loc[i, '広告目的']}"
    )
    selected_row = target_df.loc[selected_index]

    with st.expander("🔧 このKPIを編集/削除", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        edit_ad_media = col1.selectbox("広告媒体", options=広告媒体一覧, index=広告媒体一覧.index(selected_row["広告媒体"]), key="edit_admedia")
        edit_main_cat = col2.selectbox("メインカテゴリ", options=メインカテゴリ一覧, index=メインカテゴリ一覧.index(selected_row["メインカテゴリ"]), key="edit_maincat")
        edit_sub_cat = col3.selectbox("サブカテゴリ", options=サブカテゴリ一覧, index=サブカテゴリ一覧.index(selected_row["サブカテゴリ"]), key="edit_subcat")
        edit_obj = col4.selectbox("広告目的", options=広告目的一覧, index=広告目的一覧.index(selected_row["広告目的"]), key="edit_obj")

        cols = st.columns(9)
        edit_cpa_best = cols[0].number_input("CPA_best", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPA_best"], key="edit_cpa_best")
        edit_cpa_good = cols[1].number_input("CPA_good", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPA_good"], key="edit_cpa_good")
        edit_cpa_min = cols[2].number_input("CPA_min", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPA_min"], key="edit_cpa_min")
        edit_cvr_best = cols[3].number_input("CVR_best", min_value=0.0, step=0.01, format="%.2f", value=selected_row["CVR_best"], key="edit_cvr_best")
        edit_cvr_good = cols[4].number_input("CVR_good", min_value=0.0, step=0.01, format="%.2f", value=selected_row["CVR_good"], key="edit_cvr_good")
        edit_cvr_min = cols[5].number_input("CVR_min", min_value=0.0, step=0.01, format="%.2f", value=selected_row["CVR_min"], key="edit_cvr_min")
        edit_ctr_best = cols[6].number_input("CTR_best", min_value=0.0, step=0.01, format="%.2f", value=selected_row["CTR_best"], key="edit_ctr_best")
        edit_ctr_good = cols[7].number_input("CTR_good", min_value=0.0, step=0.01, format="%.2f", value=selected_row["CTR_good"], key="edit_ctr_good")
        edit_ctr_min = cols[8].number_input("CTR_min", min_value=0.0, step=0.01, format="%.2f", value=selected_row["CTR_min"], key="edit_ctr_min")
        cols2 = st.columns(9)
        edit_cpc_best = cols2[0].number_input("CPC_best", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPC_best"], key="edit_cpc_best")
        edit_cpc_good = cols2[1].number_input("CPC_good", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPC_good"], key="edit_cpc_good")
        edit_cpc_min = cols2[2].number_input("CPC_min", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPC_min"], key="edit_cpc_min")
        edit_cpm_best = cols2[3].number_input("CPM_best", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPM_best"], key="edit_cpm_best")
        edit_cpm_good = cols2[4].number_input("CPM_good", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPM_good"], key="edit_cpm_good")
        edit_cpm_min = cols2[5].number_input("CPM_min", min_value=0.0, step=1.0, format="%.0f", value=selected_row["CPM_min"], key="edit_cpm_min")

        col_edit, col_delete = st.columns([1,1])
        if col_edit.button("📝 この内容で更新", key="update_btn"):
            # 行更新
            updated_row = {
                "広告媒体": edit_ad_media,
                "メインカテゴリ": edit_main_cat,
                "サブカテゴリ": edit_sub_cat,
                "広告目的": edit_obj,
                "CPA_best": edit_cpa_best, "CPA_good": edit_cpa_good, "CPA_min": edit_cpa_min,
                "CVR_best": edit_cvr_best, "CVR_good": edit_cvr_good, "CVR_min": edit_cvr_min,
                "CTR_best": edit_ctr_best, "CTR_good": edit_ctr_good, "CTR_min": edit_ctr_min,
                "CPC_best": edit_cpc_best, "CPC_good": edit_cpc_good, "CPC_min": edit_cpc_min,
                "CPM_best": edit_cpm_best, "CPM_good": edit_cpm_good, "CPM_min": edit_cpm_min
            }
            st.session_state.target_df.loc[selected_index] = updated_row
            st.success("✅ 更新しました（保存は下のボタンで）")
            st.experimental_rerun()
        if col_delete.button("🗑️ この行を削除", key="delete_btn"):
            st.session_state.target_df = target_df.drop(selected_index).reset_index(drop=True)
            st.success("🗑️ 削除しました（保存は下のボタンで）")
            st.experimental_rerun()

# --- 一覧（表示のみ）---
st.dataframe(st.session_state.target_df, use_container_width=True)

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
            save_df = st.session_state.target_df[save_columns]
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
