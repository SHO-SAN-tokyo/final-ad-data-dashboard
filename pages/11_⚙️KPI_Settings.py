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

# --- ユニーク値取得 ---
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

if "kpi_df" not in st.session_state:
    st.session_state.kpi_df = load_target_data()
kpi_df = st.session_state.kpi_df

# --- 追加フォーム ---
st.markdown("### 🎯 新しいKPIを追加")
# --- ユニークな組み合わせから既存を除外する ---
all_combinations = pd.DataFrame([
    (m, main, sub, obj)
    for m in 広告媒体一覧
    for main in メインカテゴリ一覧
    for sub in サブカテゴリ一覧
    for obj in 広告目的一覧
], columns=["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"])

# 既存と重複しない組み合わせを抽出
existing_keys = set(tuple(x) for x in kpi_df[["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"]].values)
available_combinations = all_combinations[~all_combinations.apply(tuple, axis=1).isin(existing_keys)]

# 空なら警告
if available_combinations.empty:
    st.info("✅ すべての組み合わせが登録済みです。")
else:
    st.markdown("### 🎯 新しいKPIを追加")
    with st.form("add_kpi_form"):
        col1, col2, col3, col4 = st.columns(4)
        ad_media = col1.selectbox("広告媒体", available_combinations["広告媒体"].unique())
        filtered_maincat = available_combinations[available_combinations["広告媒体"] == ad_media]["メインカテゴリ"].unique()
        main_cat = col2.selectbox("メインカテゴリ", filtered_maincat)

        filtered_subcat = available_combinations[
            (available_combinations["広告媒体"] == ad_media) &
            (available_combinations["メインカテゴリ"] == main_cat)
        ]["サブカテゴリ"].unique()
        sub_cat = col3.selectbox("サブカテゴリ", filtered_subcat)

        filtered_obj = available_combinations[
            (available_combinations["広告媒体"] == ad_media) &
            (available_combinations["メインカテゴリ"] == main_cat) &
            (available_combinations["サブカテゴリ"] == sub_cat)
        ]["広告目的"].unique()
        obj = col4.selectbox("広告目的", filtered_obj)


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
        is_dup = (
            (kpi_df["広告媒体"] == ad_media) &
            (kpi_df["メインカテゴリ"] == main_cat) &
            (kpi_df["サブカテゴリ"] == sub_cat) &
            (kpi_df["広告目的"] == obj)
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
            st.session_state.kpi_df = pd.concat([kpi_df, new_row], ignore_index=True)
            st.success("✅ 新しいKPIを追加しました（※保存は下のボタンで）")

# --- 編集対象選択 ---
st.markdown("### 🛠 KPI編集／削除")
kpi_df = st.session_state.kpi_df
if not kpi_df.empty:
    show_df = kpi_df.copy()
    show_df.index = range(1, len(show_df) + 1)
    show_df_display = show_df[
        ["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的",
         "CPA_best", "CPA_good", "CPA_min",
         "CVR_best", "CVR_good", "CVR_min",
         "CTR_best", "CTR_good", "CTR_min",
         "CPC_best", "CPC_good", "CPC_min",
         "CPM_best", "CPM_good", "CPM_min"]]
    st.dataframe(show_df_display, use_container_width=True)

    edit_idx = st.number_input("編集/削除したい行番号を選択", min_value=1, max_value=len(show_df), step=1)
    edit_row = show_df.iloc[edit_idx - 1]

    with st.expander(f"📝 この行を編集・削除（No.{edit_idx}）", expanded=False):
        # 編集フォーム
        col1, col2, col3, col4 = st.columns(4)
        edit_media = col1.selectbox("広告媒体", options=広告媒体一覧, index=広告媒体一覧.index(edit_row["広告媒体"]), key="edit_media")
        edit_main_cat = col2.selectbox("メインカテゴリ", options=メインカテゴリ一覧, index=メインカテゴリ一覧.index(edit_row["メインカテゴリ"]), key="edit_maincat")
        edit_sub_cat = col3.selectbox("サブカテゴリ", options=サブカテゴリ一覧, index=サブカテゴリ一覧.index(edit_row["サブカテゴリ"]), key="edit_subcat")
        edit_obj = col4.selectbox("広告目的", options=広告目的一覧, index=広告目的一覧.index(edit_row["広告目的"]), key="edit_obj")

        cols = st.columns(9)
        edit_cpa_best = cols[0].number_input("CPA_best", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPA_best"], key="edit_cpa_best")
        edit_cpa_good = cols[1].number_input("CPA_good", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPA_good"], key="edit_cpa_good")
        edit_cpa_min = cols[2].number_input("CPA_min", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPA_min"], key="edit_cpa_min")
        edit_cvr_best = cols[3].number_input("CVR_best", min_value=0.0, step=0.01, format="%.2f", value=edit_row["CVR_best"], key="edit_cvr_best")
        edit_cvr_good = cols[4].number_input("CVR_good", min_value=0.0, step=0.01, format="%.2f", value=edit_row["CVR_good"], key="edit_cvr_good")
        edit_cvr_min = cols[5].number_input("CVR_min", min_value=0.0, step=0.01, format="%.2f", value=edit_row["CVR_min"], key="edit_cvr_min")
        edit_ctr_best = cols[6].number_input("CTR_best", min_value=0.0, step=0.01, format="%.2f", value=edit_row["CTR_best"], key="edit_ctr_best")
        edit_ctr_good = cols[7].number_input("CTR_good", min_value=0.0, step=0.01, format="%.2f", value=edit_row["CTR_good"], key="edit_ctr_good")
        edit_ctr_min = cols[8].number_input("CTR_min", min_value=0.0, step=0.01, format="%.2f", value=edit_row["CTR_min"], key="edit_ctr_min")
        cols2 = st.columns(9)
        edit_cpc_best = cols2[0].number_input("CPC_best", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPC_best"], key="edit_cpc_best")
        edit_cpc_good = cols2[1].number_input("CPC_good", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPC_good"], key="edit_cpc_good")
        edit_cpc_min = cols2[2].number_input("CPC_min", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPC_min"], key="edit_cpc_min")
        edit_cpm_best = cols2[3].number_input("CPM_best", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPM_best"], key="edit_cpm_best")
        edit_cpm_good = cols2[4].number_input("CPM_good", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPM_good"], key="edit_cpm_good")
        edit_cpm_min = cols2[5].number_input("CPM_min", min_value=0.0, step=1.0, format="%.0f", value=edit_row["CPM_min"], key="edit_cpm_min")

        # 編集ボタン
        if st.button("この内容で上書き保存", key="edit_save_btn"):
            # 重複チェック（同じ組み合わせが他にないか）
            is_dup = (
                (kpi_df["広告媒体"] == edit_media) &
                (kpi_df["メインカテゴリ"] == edit_main_cat) &
                (kpi_df["サブカテゴリ"] == edit_sub_cat) &
                (kpi_df["広告目的"] == edit_obj)
            )
            # 自分自身の行だけ許容
            if is_dup.sum() > 1:
                st.warning("⚠️ この組み合わせは既に他の行で登録されています。")
            else:
                # 行を上書き
                kpi_df.iloc[edit_idx - 1] = [
                    edit_media, edit_main_cat, edit_sub_cat, edit_obj,
                    edit_cpa_best, edit_cpa_good, edit_cpa_min,
                    edit_cvr_best, edit_cvr_good, edit_cvr_min,
                    edit_ctr_best, edit_ctr_good, edit_ctr_min,
                    edit_cpc_best, edit_cpc_good, edit_cpc_min,
                    edit_cpm_best, edit_cpm_good, edit_cpm_min
                ]
                st.session_state.kpi_df = kpi_df
                st.success("✅ 行を編集しました（※保存は下のボタンで）")

        # 削除ボタン
        if st.button("この行を削除する", key="del_btn"):
            confirm = st.checkbox("本当に削除しますか？（チェックで有効）", key="del_confirm")
            if confirm:
                kpi_df = kpi_df.drop(kpi_df.index[edit_idx - 1]).reset_index(drop=True)
                st.session_state.kpi_df = kpi_df
                st.success("✅ 行を削除しました（※保存は下のボタンで）")

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
            save_df = st.session_state.kpi_df[save_columns]
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
