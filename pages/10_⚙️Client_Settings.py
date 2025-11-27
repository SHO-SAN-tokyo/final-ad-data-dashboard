# final-ad-data-dashboard/pages/10_⚙️Client_Settings.py
# streamlit版のAd Driveのクライアント設定ページ
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import random
import string

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

# ──────────────────────────────────────────────
# クライアント設定
# ──────────────────────────────────────────────
st.set_page_config(page_title="クライアント設定", layout="wide")
st.title("⚙️ クライアント設定")

# --- BigQuery 認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- テーブル情報 ---
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "ClientSettings"
full_table = f"{project_id}.{dataset}.{table}"

# 追加カラム名をまとめて定義
NEW_COLS = [
    "report_display",          # レポート表示（予算 / 消化金額）
    "meta_manager_urls",       # Meta広告マネージャーURL（最大6件、改行区切り）
    "google_manager_urls",     # Google広告マネージャーURL（最大3件、改行区切り）
    "line_manager_urls",       # LINE広告マネージャーURL（最大3件、改行区切り）
    "other_manager_urls",      # その他広告マネージャーURL（最大3件、改行区切り）
]

def generate_random_suffix(length=30):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def normalize_urls(text: str, max_count: int) -> str:
    """
    テキストエリアからのURL文字列を正規化。
    - 改行で分割
    - 空行は除外
    - 先頭 max_count 件に制限
    - 再度改行区切りで結合
    """
    if not text:
        return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines[:max_count])

# --- クライアント一覧取得 ---
@st.cache_data(ttl=60)
def load_clients():
    query = f"""
    SELECT DISTINCT client_name 
    FROM {project_id}.{dataset}.Final_Ad_Data
    WHERE client_name IS NOT NULL AND client_name != ''
    ORDER BY client_name
    """
    return client.query(query).to_dataframe()

# --- 登録済み設定取得 ---
@st.cache_data(ttl=60)
def load_client_settings():
    query = f"SELECT * FROM {full_table}"
    return client.query(query).to_dataframe()

clients_df = load_clients()
settings_df = load_client_settings()

# 既存テーブルに新カラムが無い場合は DataFrame 側で空列を追加しておく
for col in NEW_COLS:
    if col not in settings_df.columns:
        settings_df[col] = ""

# --- 未登録クライアント取得 ---
registered_clients = set(settings_df["client_name"]) if not settings_df.empty else set()
unregistered_df = clients_df[~clients_df["client_name"].isin(registered_clients)]

# --- 新規登録 ---
st.markdown("### ➕ 新しいクライアントを登録")
if unregistered_df.empty:
    st.info("✅ 登録可能な新規クライアントはありません")
else:
    selected_client = st.selectbox("👤 クライアント名を選択", unregistered_df["client_name"])
    client_id_prefix = st.text_input("🆔 クライアントIDを入力 (クライアントID完成例: livebest_ランダム文字列)")

    if "random_suffix" not in st.session_state:
        st.session_state["random_suffix"] = generate_random_suffix()

    st.markdown("📋 下のランダム文字列をコピーして、クライアントIDの末尾に貼り付ける：")
    st.code(f"_{st.session_state['random_suffix']}", language="plaintext")

    # 棟数セグメント
    building_count = st.selectbox(
        "🏠 棟数セグメント",
        ["", "超ヘビー(200棟以上)", "ヘビー(50棟以上)", "M1(26棟~50棟)", 
         "M2(10棟~25棟)", "ライト(10棟以下)", "その他(棟数概念なしなど)"]
    )

    # 事業内容（複数選択）
    business_options = ["注文住宅", "規格住宅", "リフォーム", "リノベーション",
                        "分譲住宅", "分譲マンション", "土地", "賃貸", "中古物件", "その他"]
    business_selected = st.multiselect(
        "💼 事業内容（複数選択可）",
        options=business_options
    )
    business_content = ",".join(business_selected)

    # 注力度
    focus_level = st.text_input("🚀 注力度")

    # レポート表示
    report_display_options = ["", "予算", "消化金額"]
    report_display = st.selectbox("📊 レポート表示", report_display_options, index=0)

    # 広告マネージャーURL（1行1URLで入力）
    meta_manager_urls_text = st.text_area(
        "📘 Meta広告マネージャーURL（1行1URL、最大6件）",
        value="",
        height=100
    )
    google_manager_urls_text = st.text_area(
        "🔎 Google広告マネージャーURL（1行1URL、最大3件）",
        value="",
        height=100
    )
    line_manager_urls_text = st.text_area(
        "💬 LINE広告マネージャーURL（1行1URL、最大3件）",
        value="",
        height=100
    )
    other_manager_urls_text = st.text_area(
        "📂 その他広告マネージャーURL（1行1URL、最大3件）",
        value="",
        height=100
    )

    if st.button("＋ クライアントを登録"):
        if selected_client and client_id_prefix:
            client_id = f"{client_id_prefix}_{st.session_state['random_suffix']}"

            meta_manager_urls = normalize_urls(meta_manager_urls_text, max_count=6)
            google_manager_urls = normalize_urls(google_manager_urls_text, max_count=3)
            line_manager_urls = normalize_urls(line_manager_urls_text, max_count=3)
            other_manager_urls = normalize_urls(other_manager_urls_text, max_count=3)

            new_row = pd.DataFrame([{
                "client_name": selected_client,
                "client_id": client_id,
                "building_count": building_count,
                "buisiness_content": business_content,
                "focus_level": focus_level,
                "report_display": report_display,
                "meta_manager_urls": meta_manager_urls,
                "google_manager_urls": google_manager_urls,
                "line_manager_urls": line_manager_urls,
                "other_manager_urls": other_manager_urls,
                "created_at": datetime.now()
            }])

            # 既存 DF にも新カラムがあることを再度保証
            for col in NEW_COLS:
                if col not in settings_df.columns:
                    settings_df[col] = ""

            updated_df = pd.concat([settings_df, new_row], ignore_index=True)

            try:
                with st.spinner("保存中..."):
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("client_name", "STRING"),
                            bigquery.SchemaField("client_id", "STRING"),
                            bigquery.SchemaField("building_count", "STRING"),
                            bigquery.SchemaField("buisiness_content", "STRING"),
                            bigquery.SchemaField("focus_level", "STRING"),
                            bigquery.SchemaField("report_display", "STRING"),
                            bigquery.SchemaField("meta_manager_urls", "STRING"),
                            bigquery.SchemaField("google_manager_urls", "STRING"),
                            bigquery.SchemaField("line_manager_urls", "STRING"),
                            bigquery.SchemaField("other_manager_urls", "STRING"),
                            bigquery.SchemaField("created_at", "TIMESTAMP"),
                        ]
                    )
                    job = client.load_table_from_dataframe(updated_df, full_table, job_config=job_config)
                    job.result()
                    st.success(f"✅ {selected_client} を登録しました！")
                    st.cache_data.clear()
                    del st.session_state["random_suffix"]
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")
        else:
            st.warning("⚠️ クライアントIDを入力してください")

# --- クライアント情報の編集 ---
st.markdown("---")
st.markdown("### 📝 既存クライアントの編集")

if settings_df.empty:
    st.info("❗まだ登録されたクライアントはありません")
else:
    client_names = settings_df["client_name"].unique().tolist()
    selected_name = st.selectbox("👤 編集するクライアントを選択", ["--- 選択してください ---"] + client_names)

    if selected_name != "--- 選択してください ---":
        row = settings_df[settings_df["client_name"] == selected_name].iloc[0]

        with st.form("edit_form"):
            updated_client_id = st.text_input("🆔 クライアントID", value=row["client_id"])

            # 棟数セグメント
            building_options = ["", "超ヘビー(200棟以上)", "ヘビー(50棟以上)", "M1(26棟~50棟)", 
                                "M2(10棟~25棟)", "ライト(10棟以下)", "その他(棟数概念なしなど)"]
            updated_building_count = st.selectbox(
                "🏠 棟数セグメント",
                building_options,
                index=building_options.index(row["building_count"]) if row["building_count"] in building_options else 0
            )

            # 事業内容（既存値を分割＆不正値は無視）
            business_options = ["注文住宅", "規格住宅", "リフォーム", "リノベーション",
                                "分譲住宅", "分譲マンション", "土地", "賃貸", "中古物件", "その他"]
            current_business_list = row["buisiness_content"].split(",") if pd.notna(row["buisiness_content"]) else []
            current_business_list = [opt for opt in current_business_list if opt in business_options]

            updated_business_selected = st.multiselect(
                "💼 事業内容（複数選択可）",
                options=business_options,
                default=current_business_list
            )
            updated_business_content = ",".join(updated_business_selected)

            # 注力度
            updated_focus_level = st.text_input("🚀 注力度", value=row["focus_level"])

            # レポート表示
            report_display_options = ["", "予算", "消化金額"]
            current_report_display = row.get("report_display", "") if isinstance(row, pd.Series) else ""
            updated_report_display = st.selectbox(
                "📊 レポート表示",
                report_display_options,
                index=report_display_options.index(current_report_display) if current_report_display in report_display_options else 0
            )

            # 広告マネージャーURL（既存値をテキストエリアに反映）
            meta_manager_urls_existing = row.get("meta_manager_urls", "") if isinstance(row, pd.Series) else ""
            google_manager_urls_existing = row.get("google_manager_urls", "") if isinstance(row, pd.Series) else ""
            line_manager_urls_existing = row.get("line_manager_urls", "") if isinstance(row, pd.Series) else ""
            other_manager_urls_existing = row.get("other_manager_urls", "") if isinstance(row, pd.Series) else ""

            updated_meta_manager_urls_text = st.text_area(
                "📘 Meta広告マネージャーURL（1行1URL、最大6件）",
                value=meta_manager_urls_existing or "",
                height=100
            )
            updated_google_manager_urls_text = st.text_area(
                "🔎 Google広告マネージャーURL（1行1URL、最大3件）",
                value=google_manager_urls_existing or "",
                height=100
            )
            updated_line_manager_urls_text = st.text_area(
                "💬 LINE広告マネージャーURL（1行1URL、最大3件）",
                value=line_manager_urls_existing or "",
                height=100
            )
            updated_other_manager_urls_text = st.text_area(
                "📂 その他広告マネージャーURL（1行1URL、最大3件）",
                value=other_manager_urls_existing or "",
                height=100
            )

            submitted = st.form_submit_button("💾 保存")

        # 保存処理はフォーム外
        if submitted:
            try:
                # URLを正規化
                updated_meta_manager_urls = normalize_urls(updated_meta_manager_urls_text, max_count=6)
                updated_google_manager_urls = normalize_urls(updated_google_manager_urls_text, max_count=3)
                updated_line_manager_urls = normalize_urls(updated_line_manager_urls_text, max_count=3)
                updated_other_manager_urls = normalize_urls(updated_other_manager_urls_text, max_count=3)

                # DataFrame に新カラムが存在することを保証
                for col in NEW_COLS:
                    if col not in settings_df.columns:
                        settings_df[col] = ""

                settings_df.loc[settings_df["client_name"] == selected_name, [
                    "client_id",
                    "building_count",
                    "buisiness_content",
                    "focus_level",
                    "report_display",
                    "meta_manager_urls",
                    "google_manager_urls",
                    "line_manager_urls",
                    "other_manager_urls"
                ]] = [
                    updated_client_id,
                    updated_building_count,
                    updated_business_content,
                    updated_focus_level,
                    updated_report_display,
                    updated_meta_manager_urls,
                    updated_google_manager_urls,
                    updated_line_manager_urls,
                    updated_other_manager_urls
                ]

                with st.spinner("保存中..."):
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        schema=[
                            bigquery.SchemaField("client_name", "STRING"),
                            bigquery.SchemaField("client_id", "STRING"),
                            bigquery.SchemaField("building_count", "STRING"),
                            bigquery.SchemaField("buisiness_content", "STRING"),
                            bigquery.SchemaField("focus_level", "STRING"),
                            bigquery.SchemaField("report_display", "STRING"),
                            bigquery.SchemaField("meta_manager_urls", "STRING"),
                            bigquery.SchemaField("google_manager_urls", "STRING"),
                            bigquery.SchemaField("line_manager_urls", "STRING"),
                            bigquery.SchemaField("other_manager_urls", "STRING"),
                            bigquery.SchemaField("created_at", "TIMESTAMP"),
                        ]
                    )
                    job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                    job.result()
                    st.success("✅ 保存が完了しました！")
                    st.cache_data.clear()
                    settings_df = load_client_settings()
                    # 再読込後も新カラムを保証
                    for col in NEW_COLS:
                        if col not in settings_df.columns:
                            settings_df[col] = ""
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")

        with st.expander("🗑 このクライアント情報を削除"):
            if st.button("❌ クライアントを削除"):
                try:
                    settings_df = settings_df[settings_df["client_name"] != selected_name]

                    # 削除時も新カラムを保証
                    for col in NEW_COLS:
                        if col not in settings_df.columns:
                            settings_df[col] = ""

                    with st.spinner("削除中..."):
                        job_config = bigquery.LoadJobConfig(
                            write_disposition="WRITE_TRUNCATE",
                            schema=[
                                bigquery.SchemaField("client_name", "STRING"),
                                bigquery.SchemaField("client_id", "STRING"),
                                bigquery.SchemaField("building_count", "STRING"),
                                bigquery.SchemaField("buisiness_content", "STRING"),
                                bigquery.SchemaField("focus_level", "STRING"),
                                bigquery.SchemaField("report_display", "STRING"),
                                bigquery.SchemaField("meta_manager_urls", "STRING"),
                                bigquery.SchemaField("google_manager_urls", "STRING"),
                                bigquery.SchemaField("line_manager_urls", "STRING"),
                                bigquery.SchemaField("other_manager_urls", "STRING"),
                                bigquery.SchemaField("created_at", "TIMESTAMP"),
                            ]
                        )
                        job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                        job.result()
                        st.success("🗑 削除が完了しました")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ 削除エラー: {e}")

# --- クライアント別リンク一覧 ---
st.markdown("---")
st.markdown("### 🔗 クライアント別ページリンク（一覧表示）")

if settings_df.empty:
    st.info("❗登録されたクライアントがありません")
else:
    # 念のため新カラムを保証（ここでは使わないがスキーマ整合のため）
    for col in NEW_COLS:
        if col not in settings_df.columns:
            settings_df[col] = ""

    link_df = settings_df[["client_name", "building_count", "buisiness_content", "focus_level", "client_id"]].copy()
    link_df["リンクURL"] = link_df["client_id"].apply(
        lambda cid: f"https://sho-san-client-ad-score.streamlit.app/?client_id={cid}"
    )

    st.divider()

    header_cols = st.columns([2, 2, 1, 1.5, 1.5])
    header_cols[0].markdown("**クライアント名**")
    header_cols[1].markdown("**リンク**")
    header_cols[2].markdown("**注力度**")
    header_cols[3].markdown("**事業内容**")
    header_cols[4].markdown("**棟数セグメント**")

    st.divider()

    for _, row in link_df.iterrows():
        cols = st.columns([2, 2, 1, 1.5, 1.5])
        row_height = "70px"
        row_style = f"border-bottom: 1px solid #ddd; height: {row_height}; min-height: {row_height}; display: flex; align-items: center;"

        with cols[0]:
            st.markdown(f'<div style="{row_style}">{row["client_name"]}</div>', unsafe_allow_html=True)
        with cols[1]:
            button_html = f"""
            <a href="{row['リンクURL']}" target="_blank" style="
                text-decoration: none;
                display: inline-block;
                padding: 0.3em 0.8em;
                border-radius: 6px;
                background-color: rgb(53, 169, 195);
                color: white;
                font-weight: bold;">
                ▶ ページを開く
            </a>
            """
            st.markdown(f'<div style="{row_style}">{button_html}</div>', unsafe_allow_html=True)
        with cols[2]:
            st.markdown(f'<div style="{row_style}">{row["focus_level"] or "&nbsp;"} </div>', unsafe_allow_html=True)
        with cols[3]:
            st.markdown(f'<div style="{row_style}">{row["buisiness_content"] or "&nbsp;"} </div>', unsafe_allow_html=True)
        with cols[4]:
            st.markdown(f'<div style="{row_style}">{row["building_count"] or "&nbsp;"} </div>', unsafe_allow_html=True)
