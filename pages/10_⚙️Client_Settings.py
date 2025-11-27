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

# URL 用のカラム名
URL_COLS = [
    # Meta (max 6)
    "meta_manager_url_1",
    "meta_manager_url_2",
    "meta_manager_url_3",
    "meta_manager_url_4",
    "meta_manager_url_5",
    "meta_manager_url_6",
    # Google (max 3)
    "google_manager_url_1",
    "google_manager_url_2",
    "google_manager_url_3",
    # LINE (max 3)
    "line_manager_url_1",
    "line_manager_url_2",
    "line_manager_url_3",
    # Other (max 3)
    "other_manager_url_1",
    "other_manager_url_2",
    "other_manager_url_3",
]

# 追加カラム名まとめ
NEW_COLS = ["report_display"] + URL_COLS

def generate_random_suffix(length=30):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

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

# ──────────────────────────────────────────────
# 新規登録
# ──────────────────────────────────────────────
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

    # ───────────────────────────────
    # 広告マネージャーURL（1URL = 1フィールド）
    # ───────────────────────────────
    st.markdown("#### 🔍 Meta広告マネージャー（1URLにつき1フィールド、最大6件）")
    meta_url_inputs = []
    for i in range(6):
        meta_url_inputs.append(
            st.text_input(f"Meta URL {i+1}", key=f"meta_new_{i}")
        )

    st.markdown("#### 🔎 Google広告マネージャー（1URLにつき1フィールド、最大3件）")
    google_url_inputs = []
    for i in range(3):
        google_url_inputs.append(
            st.text_input(f"Google URL {i+1}", key=f"google_new_{i}")
        )

    st.markdown("#### 💬 LINE広告マネージャー（1URLにつき1フィールド、最大3件）")
    line_url_inputs = []
    for i in range(3):
        line_url_inputs.append(
            st.text_input(f"LINE URL {i+1}", key=f"line_new_{i}")
        )

    st.markdown("#### 📂 その他広告マネージャー（1URLにつき1フィールド、最大3件）")
    other_url_inputs = []
    for i in range(3):
        other_url_inputs.append(
            st.text_input(f"その他 URL {i+1}", key=f"other_new_{i}")
        )

    if st.button("＋ クライアントを登録"):
        if selected_client and client_id_prefix:
            client_id = f"{client_id_prefix}_{st.session_state['random_suffix']}"

            def clean(v: str) -> str:
                return v.strip() if isinstance(v, str) else ""

            new_row_dict = {
                "client_name": selected_client,
                "client_id": client_id,
                "building_count": building_count,
                "buisiness_content": business_content,
                "focus_level": focus_level,
                "report_display": report_display,
                "created_at": datetime.now(),
            }

            # Meta
            for i in range(6):
                col = f"meta_manager_url_{i+1}"
                new_row_dict[col] = clean(meta_url_inputs[i]) if i < len(meta_url_inputs) else ""
            # Google
            for i in range(3):
                col = f"google_manager_url_{i+1}"
                new_row_dict[col] = clean(google_url_inputs[i]) if i < len(google_url_inputs) else ""
            # LINE
            for i in range(3):
                col = f"line_manager_url_{i+1}"
                new_row_dict[col] = clean(line_url_inputs[i]) if i < len(line_url_inputs) else ""
            # Other
            for i in range(3):
                col = f"other_manager_url_{i+1}"
                new_row_dict[col] = clean(other_url_inputs[i]) if i < len(other_url_inputs) else ""

            new_row = pd.DataFrame([new_row_dict])

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
                            # Meta
                            bigquery.SchemaField("meta_manager_url_1", "STRING"),
                            bigquery.SchemaField("meta_manager_url_2", "STRING"),
                            bigquery.SchemaField("meta_manager_url_3", "STRING"),
                            bigquery.SchemaField("meta_manager_url_4", "STRING"),
                            bigquery.SchemaField("meta_manager_url_5", "STRING"),
                            bigquery.SchemaField("meta_manager_url_6", "STRING"),
                            # Google
                            bigquery.SchemaField("google_manager_url_1", "STRING"),
                            bigquery.SchemaField("google_manager_url_2", "STRING"),
                            bigquery.SchemaField("google_manager_url_3", "STRING"),
                            # LINE
                            bigquery.SchemaField("line_manager_url_1", "STRING"),
                            bigquery.SchemaField("line_manager_url_2", "STRING"),
                            bigquery.SchemaField("line_manager_url_3", "STRING"),
                            # Other
                            bigquery.SchemaField("other_manager_url_1", "STRING"),
                            bigquery.SchemaField("other_manager_url_2", "STRING"),
                            bigquery.SchemaField("other_manager_url_3", "STRING"),
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

# ──────────────────────────────────────────────
# 既存クライアントの編集
# ──────────────────────────────────────────────
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
            current_report_display = row["report_display"] if "report_display" in row.index else ""
            updated_report_display = st.selectbox(
                "📊 レポート表示",
                report_display_options,
                index=report_display_options.index(current_report_display) if current_report_display in report_display_options else 0
            )

            # URL 既存値をそのままフィールドに
            def get_safe(col):
                return row[col] if col in row.index and pd.notna(row[col]) else ""

            st.markdown("#### 🔍 Meta広告マネージャー（1URLにつき1フィールド、最大6件）")
            updated_meta_inputs = []
            for i in range(6):
                col = f"meta_manager_url_{i+1}"
                updated_meta_inputs.append(
                    st.text_input(
                        f"Meta URL {i+1}",
                        value=get_safe(col),
                        key=f"meta_edit_{i}"
                    )
                )

            st.markdown("#### 🔎 Google広告マネージャー（1URLにつき1フィールド、最大3件）")
            updated_google_inputs = []
            for i in range(3):
                col = f"google_manager_url_{i+1}"
                updated_google_inputs.append(
                    st.text_input(
                        f"Google URL {i+1}",
                        value=get_safe(col),
                        key=f"google_edit_{i}"
                    )
                )

            st.markdown("#### 💬 LINE広告マネージャー（1URLにつき1フィールド、最大3件）")
            updated_line_inputs = []
            for i in range(3):
                col = f"line_manager_url_{i+1}"
                updated_line_inputs.append(
                    st.text_input(
                        f"LINE URL {i+1}",
                        value=get_safe(col),
                        key=f"line_edit_{i}"
                    )
                )

            st.markdown("#### 📂 その他広告マネージャー（1URLにつき1フィールド、最大3件）")
            updated_other_inputs = []
            for i in range(3):
                col = f"other_manager_url_{i+1}"
                updated_other_inputs.append(
                    st.text_input(
                        f"その他 URL {i+1}",
                        value=get_safe(col),
                        key=f"other_edit_{i}"
                    )
                )

            submitted = st.form_submit_button("💾 保存")

        # 保存処理はフォーム外
        if submitted:
            try:
                def clean(v: str) -> str:
                    return v.strip() if isinstance(v, str) else ""

                # DataFrame に新カラムが存在することを保証
                for col in NEW_COLS:
                    if col not in settings_df.columns:
                        settings_df[col] = ""

                mask = settings_df["client_name"] == selected_name

                # 基本情報
                settings_df.loc[mask, "client_id"] = updated_client_id
                settings_df.loc[mask, "building_count"] = updated_building_count
                settings_df.loc[mask, "buisiness_content"] = updated_business_content
                settings_df.loc[mask, "focus_level"] = updated_focus_level
                settings_df.loc[mask, "report_display"] = updated_report_display

                # URL（空欄は ""）
                for i in range(6):
                    col = f"meta_manager_url_{i+1}"
                    settings_df.loc[mask, col] = clean(updated_meta_inputs[i]) if i < len(updated_meta_inputs) else ""
                for i in range(3):
                    col = f"google_manager_url_{i+1}"
                    settings_df.loc[mask, col] = clean(updated_google_inputs[i]) if i < len(updated_google_inputs) else ""
                for i in range(3):
                    col = f"line_manager_url_{i+1}"
                    settings_df.loc[mask, col] = clean(updated_line_inputs[i]) if i < len(updated_line_inputs) else ""
                for i in range(3):
                    col = f"other_manager_url_{i+1}"
                    settings_df.loc[mask, col] = clean(updated_other_inputs[i]) if i < len(updated_other_inputs) else ""

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
                            bigquery.SchemaField("meta_manager_url_1", "STRING"),
                            bigquery.SchemaField("meta_manager_url_2", "STRING"),
                            bigquery.SchemaField("meta_manager_url_3", "STRING"),
                            bigquery.SchemaField("meta_manager_url_4", "STRING"),
                            bigquery.SchemaField("meta_manager_url_5", "STRING"),
                            bigquery.SchemaField("meta_manager_url_6", "STRING"),
                            bigquery.SchemaField("google_manager_url_1", "STRING"),
                            bigquery.SchemaField("google_manager_url_2", "STRING"),
                            bigquery.SchemaField("google_manager_url_3", "STRING"),
                            bigquery.SchemaField("line_manager_url_1", "STRING"),
                            bigquery.SchemaField("line_manager_url_2", "STRING"),
                            bigquery.SchemaField("line_manager_url_3", "STRING"),
                            bigquery.SchemaField("other_manager_url_1", "STRING"),
                            bigquery.SchemaField("other_manager_url_2", "STRING"),
                            bigquery.SchemaField("other_manager_url_3", "STRING"),
                            bigquery.SchemaField("created_at", "TIMESTAMP"),
                        ]
                    )
                    job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                    job.result()
                    st.success("✅ 保存が完了しました！")
                    st.cache_data.clear()
                    settings_df = load_client_settings()
                    for col in NEW_COLS:
                        if col not in settings_df.columns:
                            settings_df[col] = ""
            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")

        with st.expander("🗑 このクライアント情報を削除"):
            if st.button("❌ クライアントを削除"):
                try:
                    settings_df = settings_df[settings_df["client_name"] != selected_name]

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
                                bigquery.SchemaField("meta_manager_url_1", "STRING"),
                                bigquery.SchemaField("meta_manager_url_2", "STRING"),
                                bigquery.SchemaField("meta_manager_url_3", "STRING"),
                                bigquery.SchemaField("meta_manager_url_4", "STRING"),
                                bigquery.SchemaField("meta_manager_url_5", "STRING"),
                                bigquery.SchemaField("meta_manager_url_6", "STRING"),
                                bigquery.SchemaField("google_manager_url_1", "STRING"),
                                bigquery.SchemaField("google_manager_url_2", "STRING"),
                                bigquery.SchemaField("google_manager_url_3", "STRING"),
                                bigquery.SchemaField("line_manager_url_1", "STRING"),
                                bigquery.SchemaField("line_manager_url_2", "STRING"),
                                bigquery.SchemaField("line_manager_url_3", "STRING"),
                                bigquery.SchemaField("other_manager_url_1", "STRING"),
                                bigquery.SchemaField("other_manager_url_2", "STRING"),
                                bigquery.SchemaField("other_manager_url_3", "STRING"),
                                bigquery.SchemaField("created_at", "TIMESTAMP"),
                            ]
                        )
                        job = client.load_table_from_dataframe(settings_df, full_table, job_config=job_config)
                        job.result()
                        st.success("🗑 削除が完了しました")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ 削除エラー: {e}")

# ──────────────────────────────────────────────
# クライアント別リンク一覧
# ──────────────────────────────────────────────
st.markdown("---")
st.markdown("### 🔗 クライアント別ページリンク（一覧表示）")

if settings_df.empty:
    st.info("❗登録されたクライアントがありません")
else:
    for col in NEW_COLS:
        if col not in settings_df.columns:
            settings_df[col] = ""

    link_df = settings_df.copy()
    link_df["リンクURL"] = link_df["client_id"].apply(
        lambda cid: f"https://sho-san-client-ad-score.streamlit.app/?client_id={cid}"
    )

    # 番号用ラベル（①〜⑥）
    circled_nums = ["①", "②", "③", "④", "⑤", "⑥"]

    def build_url_links(row: pd.Series, prefix: str, max_n: int, label_prefix: str) -> str:
        """登録されているURLにだけ、ラベル付きリンクを作って<br>で連結する"""
        parts = []
        for i in range(1, max_n + 1):
            col = f"{prefix}_{i}"
            if col in row.index and isinstance(row[col], str) and row[col].strip() != "":
                num_label = circled_nums[i-1] if i-1 < len(circled_nums) else str(i)
                label = f"{label_prefix}{num_label}"
                url = row[col].strip()
                parts.append(f'<a href="{url}" target="_blank">{label}</a>')
        return "<br>".join(parts) if parts else "—"

    st.divider()

    # 列構成:
    # クライアント名 / リンク / レポート表示 / MetaURL / GoogleURL / LINEURL / その他URL / 注力度 / 事業内容 / 棟数
    header_cols = st.columns([2, 2, 1, 2, 2, 2, 2, 1, 1.5, 1.5])
    header_cols[0].markdown("**クライアント名**")
    header_cols[1].markdown("**クライアント別ページ**")
    header_cols[2].markdown("**レポート表示**")
    header_cols[3].markdown("**Meta広告マネージャーURL**")
    header_cols[4].markdown("**Google広告マネージャーURL**")
    header_cols[5].markdown("**LINE広告マネージャーURL**")
    header_cols[6].markdown("**その他広告マネージャーURL**")
    header_cols[7].markdown("**注力度**")
    header_cols[8].markdown("**事業内容**")
    header_cols[9].markdown("**棟数セグメント**")

    st.divider()

    for _, row in link_df.iterrows():
        cols = st.columns([2, 2, 1, 2, 2, 2, 2, 1, 1.5, 1.5])
        row_height = "80px"
        row_style = f"border-bottom: 1px solid #ddd; min-height: {row_height}; display: flex; align-items: center;"

        # クライアント名
        with cols[0]:
            st.markdown(f'<div style="{row_style}">{row["client_name"]}</div>', unsafe_allow_html=True)

        # クライアント別ページリンク
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

        # レポート表示
        with cols[2]:
            st.markdown(
                f'<div style="{row_style}">{row.get("report_display") or "&nbsp;"} </div>',
                unsafe_allow_html=True
            )

        # Meta広告マネージャーURL（①〜のリンク一覧）
        with cols[3]:
            meta_links_html = build_url_links(row, "meta_manager_url", 6, "Meta広告マネージャーURL")
            st.markdown(f'<div style="{row_style}">{meta_links_html}</div>', unsafe_allow_html=True)

        # Google広告マネージャーURL
        with cols[4]:
            google_links_html = build_url_links(row, "google_manager_url", 3, "Google広告マネージャーURL")
            st.markdown(f'<div style="{row_style}">{google_links_html}</div>', unsafe_allow_html=True)

        # LINE広告マネージャーURL
        with cols[5]:
            line_links_html = build_url_links(row, "line_manager_url", 3, "LINE広告マネージャーURL")
            st.markdown(f'<div style="{row_style}">{line_links_html}</div>', unsafe_allow_html=True)

        # その他広告マネージャーURL
        with cols[6]:
            other_links_html = build_url_links(row, "other_manager_url", 3, "その他広告マネージャーURL")
            st.markdown(f'<div style="{row_style}">{other_links_html}</div>', unsafe_allow_html=True)

        # 注力度
        with cols[7]:
            st.markdown(
                f'<div style="{row_style}">{row.get("focus_level") or "&nbsp;"} </div>',
                unsafe_allow_html=True
            )

        # 事業内容
        with cols[8]:
            st.markdown(
                f'<div style="{row_style}">{row.get("buisiness_content") or "&nbsp;"} </div>',
                unsafe_allow_html=True
            )

        # 棟数セグメント
        with cols[9]:
            st.markdown(
                f'<div style="{row_style}">{row.get("building_count") or "&nbsp;"} </div>',
                unsafe_allow_html=True
            )
