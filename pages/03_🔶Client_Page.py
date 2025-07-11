import streamlit as st
import pandas as pd
from google.cloud import bigquery
import html

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

# ──────────────────────────────────────────────
# クライアント別ページリンク一覧
# ──────────────────────────────────────────────
st.set_page_config(page_title="クライアント別ページリンク一覧", layout="wide")
st.title("🔗 クライアント別ページリンク一覧")

st.markdown("""
<div style="
    margin: 16px 0 8px 0;
    font-size: 15px;
    font-weight: bold;
">
    クライアント別で広告スコアが見れるページリンク一覧です。直近の日別データや全期間データも閲覧できます。
</div>
<div style="
    margin-bottom: 28px;
    font-size: 13px;
    color: #444;
">
    ⚠️「ページを開く」を押した先では、以下のログインアイパスが必要です<br>
    <span style="font-weight:600;color:#1d7290;">　  ユーザーID: adscore</span>　
    <span style="font-weight:600;color:#b65916;">　パスワード: 7n2wb86vkw</span>
</div>
""", unsafe_allow_html=True)


# --- BigQuery認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- データ取得 ---
@st.cache_data(ttl=60)
def load_client_view():
    query = """
    SELECT 
        client_name, client_id, focus_level, `現在の担当者`, `過去の担当者`, `フロント`
    FROM SHOSAN_Ad_Tokyo.Client_List_For_Page
    """
    return client.query(query).to_dataframe()

df = load_client_view()

# --- フィルターリスト ---
current_tanto_list = sorted(set(df['現在の担当者'].dropna().unique()))
front_list = sorted(set(df['フロント'].dropna().unique()))
client_list = sorted(df['client_name'].unique())
focus_list = sorted(df['focus_level'].dropna().unique())

cols = st.columns(4)
sel_tanto = cols[0].multiselect("現在の担当者", current_tanto_list, placeholder="すべて")
sel_front = cols[1].multiselect("フロント", front_list, placeholder="すべて")
sel_client = cols[2].multiselect("クライアント名", client_list, placeholder="すべて")
sel_focus = cols[3].multiselect("注力度", focus_list, placeholder="すべて")

# --- フィルター適用 ---
filtered_df = df.copy()
if sel_tanto:
    filtered_df = filtered_df[filtered_df["現在の担当者"].isin(sel_tanto)]
if sel_front:
    filtered_df = filtered_df[filtered_df["フロント"].isin(sel_front)]
if sel_client:
    filtered_df = filtered_df[filtered_df["client_name"].isin(sel_client)]
if sel_focus:
    filtered_df = filtered_df[filtered_df["focus_level"].isin(sel_focus)]


# --- リンクURL生成 ---
filtered_df["リンクURL"] = filtered_df["client_id"].apply(
    lambda cid: f"https://sho-san-client-ad-score.streamlit.app/?client_id={cid}"
)

st.divider()

# --- CSS追加（テーブル行のテキストサイズを小さく） ---
st.markdown("""
<style>
.table-row-text {
    font-size: 13px !important;
    letter-spacing: 0.02em;
}
</style>
""", unsafe_allow_html=True)

# --- ヘッダー ---
header_cols = st.columns([2, 1.5, 2, 2, 2, 1.5])
header_cols[0].markdown("**クライアント名**")
header_cols[1].markdown("**リンク**")
header_cols[2].markdown("**現在の担当者**")
header_cols[3].markdown("**過去の担当者**")
header_cols[4].markdown("**フロント**")
header_cols[5].markdown("**注力度**")

st.divider()

def vertical_center(content, height="70px"):
    safe_content = content if pd.notna(content) and str(content).strip() != "" else "&nbsp;"
    return f"""
    <div class="table-row-text" style="display: flex; align-items: center; height: {height}; min-height: {height};">
        {html.escape(safe_content) if safe_content != "&nbsp;" else safe_content}
    </div>
    """

for _, row in filtered_df.iterrows():
    cols = st.columns([2, 1.5, 2, 2, 2, 1.5])
    row_height = "70px"
    row_style = f"border-bottom: 1px solid #ddd; height: {row_height}; min-height: {row_height}; display: flex; align-items: center;"
    with cols[0]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["client_name"])}</div>', unsafe_allow_html=True)
    with cols[1]:
        button_html = f"""
        <a href="{row['リンクURL']}" target="_blank" style="
            text-decoration: none;
            display: inline-block;
            padding: 0.3em 0.8em;
            border-radius: 6px;
            background-color: rgb(53, 169, 195);
            color: white;
            font-weight: bold;
            font-size: 13px;   /* ここで小さく */
        ">
            ▶ ページを開く
        </a>
        """
        st.markdown(f'<div style="{row_style}">{button_html}</div>', unsafe_allow_html=True)
    with cols[2]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["現在の担当者"])}</div>', unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["過去の担当者"])}</div>', unsafe_allow_html=True)
    with cols[4]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["フロント"])}</div>', unsafe_allow_html=True)
    with cols[5]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["focus_level"])}</div>', unsafe_allow_html=True)

