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
target_table = "SHOSAN_Ad_Tokyo.Target_Indicators_Meta"

# --- 広告データから「広告媒体」「メインカテゴリ」「サブカテゴリ」「広告目的」を取得 ---
@st.cache_data(ttl=60)
def get_unique_values():
    try:
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
            df["広告媒体"].unique(),
            df["メインカテゴリ"].unique(),
            df["サブカテゴリ"].unique(),
            df["広告目的"].unique()
        )
    except Exception as e:
        st.error(f"❌ ユニーク値の取得に失敗しました: {e}")
        st.stop()

広告媒体一覧, メインカテゴリ一覧, サブカテゴリ一覧, 広告目的一覧 = get_unique_values()

# --- 既存の目標データ読み込み ---
@st.cache_data(ttl=60)
def load_target_data():
    try:
        query = f"SELECT * FROM `{project_id}`.`{target_table}`"
        return client.query(query).to_dataframe()
    except Exception as e:
        st.warning("⚠️ まだ目標データが存在しない可能性があります。")
        return pd.DataFrame(columns=[
            "広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的",
            "CPA_best", "CPA_good", "CPA_min",
            "CVR_best", "CVR_good", "CVR_min",
            "CTR_best", "CTR_good", "CTR_min",
            "CPC_best", "CPC_good", "CPC_min",
            "CPM_best", "CPM_good", "CPM_min"
        ])

target_df = load_target_data()

# --- 編集用の空白行追加（全組み合わせを網羅） ---
for admedia in 広告媒体一覧:
    for main_cat in メインカテゴリ一覧:
        for sub_cat in サブカテゴリ一覧:
            for obj in 広告目的一覧:
                if not (
                    (target_df["広告媒体"] == admedia) &
                    (target_df["メインカテゴリ"] == main_cat) &
                    (target_df["サブカテゴリ"] == sub_cat) &
                    (target_df["広告目的"] == obj)
                ).any():
                    target_df = pd.concat([
                        target_df,
                        pd.DataFrame([{
                            "広告媒体": admedia,
                            "メインカテゴリ": main_cat,
                            "サブカテゴリ": sub_cat,
                            "広告目的": obj,
                            "CPA_best": None, "CPA_good": None, "CPA_min": None,
                            "CVR_best": None, "CVR_good": None, "CVR_min": None,
                            "CTR_best": None, "CTR_good": None, "CTR_min": None,
                            "CPC_best": None, "CPC_good": None, "CPC_min": None,
                            "CPM_best": None, "CPM_good": None, "CPM_min": None
                        }])
                    ], ignore_index=True)

# --- 折りたたみ式のセクションごとに表示 ---
st.markdown("### 🎯 広告媒体 × メインカテゴリ × サブカテゴリ × 広告目的ごとの目標を設定")

edited_df = target_df.sort_values(
    ["広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的"]
).copy()

for metric in ["CPA", "CVR", "CTR", "CPC", "CPM"]:
    with st.expander(f"📌 {metric} の目標設定", expanded=False):
        sub_cols = [
            "広告媒体", "メインカテゴリ", "サブカテゴリ", "広告目的",
            f"{metric}_best", f"{metric}_good", f"{metric}_min"
        ]
        sub_df = edited_df[sub_cols].copy()

        # ✅ 金額系 or パーセンテージ系でフォーマット切り替え
        if metric in ["CPA", "CPC", "CPM"]:
            fmt = "¥%d"
        elif metric in ["CVR", "CTR"]:
            fmt = "%.2f %%"

        updated_df = st.data_editor(
            sub_df,
            key=f"{metric}_editor",
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "広告媒体": st.column_config.TextColumn(disabled=True),
                "メインカテゴリ": st.column_config.TextColumn(disabled=True),
                "サブカテゴリ": st.column_config.TextColumn(disabled=True),
                "広告目的": st.column_config.TextColumn(disabled=True),
                f"{metric}_best": st.column_config.NumberColumn(format=fmt),
                f"{metric}_good": st.column_config.NumberColumn(format=fmt),
                f"{metric}_min": st.column_config.NumberColumn(format=fmt),
            },
            hide_index=True
        )

        for col in [f"{metric}_best", f"{metric}_good", f"{metric}_min"]:
            edited_df[col] = updated_df[col]

# --- 保存処理 ---
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

# --- ボタンの色をカスタマイズ（CSS適用） ---
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
