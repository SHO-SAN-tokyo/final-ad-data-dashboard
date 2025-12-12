import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()


# ──────────────────────────────────────────────
# KPI設定
# ──────────────────────────────────────────────
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

# --- 利用可能な組み合わせを取得（未登録分のみ） ---
from itertools import product

# ✅ None, "None", 空白を除去したユニーク値を再定義
def clean(values):
    return sorted(v for v in values if v and str(v).strip().lower() != "none")

広告媒体一覧 = clean(広告媒体一覧)
メインカテゴリ一覧 = clean(メインカテゴリ一覧)
サブカテゴリ一覧 = clean(サブカテゴリ一覧)
広告目的一覧 = clean(広告目的一覧)

# --- 実データ上に存在するユニークな組み合わせのみ取得 ---
@st.cache_data(ttl=60)
def get_available_combinations():
    query = f"""
        SELECT DISTINCT `広告媒体`, `メインカテゴリ`, `サブカテゴリ`, `広告目的`
        FROM `{project_id}.{source_table}`
        WHERE `広告媒体` IS NOT NULL
          AND `メインカテゴリ` IS NOT NULL
          AND `サブカテゴリ` IS NOT NULL
          AND `広告目的` IS NOT NULL
    """
    return client.query(query).to_dataframe()

# 全候補を実データから取得
all_combinations = get_available_combinations()

# 👇 ここに追加（空文字・"None"文字列の除去）
def is_valid_combination(row):
    return all(v and str(v).strip().lower() != "none" for v in row)

all_combinations = all_combinations[all_combinations.apply(is_valid_combination, axis=1)].reset_index(drop=True)



# 既存との突合（未登録のものだけ残す）
existing_combinations = kpi_df[["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"]]
available_combinations = pd.merge(
    all_combinations, existing_combinations,
    on=["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"],
    how="left", indicator=True
).query('_merge == "left_only"').drop(columns=['_merge'])

# --- KPI追加フォーム（未登録の組み合わせだけ選ばせる） ---
if available_combinations.empty:
    st.info("✅ すべての組み合わせが登録済みです。")
else:
    st.markdown("### 🎯 新しいKPIを追加")
    with st.form("add_kpi_form"):
        combo_labels = available_combinations.apply(
            lambda row: f"{row['広告媒体']} | {row['メインカテゴリ']} | {row['サブカテゴリ']} | {row['広告目的']}",
            axis=1
        )
        selected_label = st.selectbox("📦 KPIを追加する組み合わせを選択", options=combo_labels)

        # 選ばれた行を取得
        selected_row = available_combinations.iloc[combo_labels.tolist().index(selected_label)]
        ad_media = selected_row["広告媒体"]
        main_cat = selected_row["メインカテゴリ"]
        sub_cat = selected_row["サブカテゴリ"]
        obj = selected_row["広告目的"]

        st.markdown("#### 指標値をすべて入力")
        cols = st.columns(9)
        cpa_best = cols[0].number_input("CPA_best", min_value=0.0, step=1.0, format="%.0f")
        cpa_good = cols[1].number_input("CPA_good", min_value=0.0, step=1.0, format="%.0f")
        cpa_min = cols[2].number_input("CPA_min", min_value=0.0, step=1.0, format="%.0f")
        cvr_best = cols[3].number_input("CVR_best", min_value=0.0, step=0.01, format="%.2f")
        cvr_good = cols[4].number_input("CVR_good", min_value=0.0, step=0.01, format="%.2f")
        cvr_min = cols[5].number_input("CVR_min", min_value=0.0, step=0.01, format="%.2f")
        ctr_best = cols[6].number_input("CTR_best", min_value=0.0, step=0.01, format="%.2f")
        ctr_good = cols[7].number_input("CTR_good", min_value=0.0, step=0.01, format="%.2f")
        ctr_min = cols[8].number_input("CTR_min", min_value=0.0, step=0.01, format="%.2f")

        cols2 = st.columns(9)
        cpc_best = cols2[0].number_input("CPC_best", min_value=0.0, step=1.0, format="%.0f")
        cpc_good = cols2[1].number_input("CPC_good", min_value=0.0, step=1.0, format="%.0f")
        cpc_min = cols2[2].number_input("CPC_min", min_value=0.0, step=1.0, format="%.0f")
        cpm_best = cols2[3].number_input("CPM_best", min_value=0.0, step=1.0, format="%.0f")
        cpm_good = cols2[4].number_input("CPM_good", min_value=0.0, step=1.0, format="%.0f")
        cpm_min = cols2[5].number_input("CPM_min", min_value=0.0, step=1.0, format="%.0f")

        submitted = st.form_submit_button("追加")
        if submitted:
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
            st.session_state.kpi_df = pd.concat([st.session_state.kpi_df, new_row], ignore_index=True)
            st.success("✅ 新しいKPIを追加しました（※保存は下のボタンで）")


# --- KPI編集／削除（テーブルを直接編集） ---
st.markdown("### 🛠 KPI編集／削除")

kpi_df = st.session_state.kpi_df

if kpi_df.empty:
    st.info("まだKPIが登録されていません。上のフォームから追加してください。")
else:
    st.info(
        "表のセルを直接編集できます。行の追加・削除も可能です（右クリックメニューなど）。\n"
        "※編集後は必ず下の「💾 保存する」ボタンで BigQuery に反映してください。",
        icon="✏️",
    )

    # data_editor で直接編集
    edited_df = st.data_editor(
        kpi_df,
        num_rows="dynamic",           # 行の追加・削除を許可
        use_container_width=True,
        hide_index=True,
        key="kpi_editor",
        column_config={
            "広告媒体": st.column_config.SelectboxColumn(
                "広告媒体", options=広告媒体一覧
            ),
            "メインカテゴリ": st.column_config.SelectboxColumn(
                "メインカテゴリ", options=メインカテゴリ一覧
            ),
            "サブカテゴリ": st.column_config.SelectboxColumn(
                "サブカテゴリ", options=サブカテゴリ一覧
            ),
            "広告目的": st.column_config.SelectboxColumn(
                "広告目的", options=広告目的一覧
            ),
        },
    )

    # 編集結果をセッションに反映
    st.session_state.kpi_df = edited_df


# --- 保存ボタン ---
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

            # 必要なカラムだけ抜き出し & インデックス整理
            save_df = st.session_state.kpi_df[save_columns].copy().reset_index(drop=True)

            # 4キーが欠けている行は念のため除外（空行対策）
            key_cols = ["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"]
            save_df = save_df.dropna(subset=key_cols)

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


