import streamlit as st
import pandas as pd
from google.cloud import bigquery
import html

# --- ページ設定 ---
st.set_page_config(page_title="クライアント別ページリンク", layout="wide")
st.title("🔗 クライアント別ページリンク（一覧表示）")

# --- BigQuery認証 ---
info = dict(st.secrets["connections"]["bigquery"])
info["private_key"] = info["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info)

# --- VIEWからデータ取得 ---
@st.cache_data(ttl=60)
def load_client_view():
    query = """
    SELECT 
        client_name, client_id, focus_level, 
        担当者一覧, フロント一覧
    FROM SHOSAN_Ad_Tokyo.Client_List_For_Page
    """
    return client.query(query).to_dataframe()

df = load_client_view()

# --- フィルター作成 ---
担当者リスト = sorted(set(sum([str(x).split(', ') for x in df['担当者一覧'] if pd.notna(x)], [])))
フロントリスト = sorted(set(sum([str(x).split(', ') for x in df['フロント一覧'] if pd.notna(x)], [])))
クライアント名リスト = sorted(df['client_name'].unique())
注力度リスト = sorted(df['focus_level'].dropna().unique())

with st.form("filter_form"):
    cols = st.columns(4)
    sel_tanto = cols[0].multiselect("担当者", 担当者リスト)
    sel_front = cols[1].multiselect("フロント", フロントリスト)
    sel_client = cols[2].multiselect("クライアント名", クライアント名リスト)
    sel_focus = cols[3].multiselect("注力度", 注力度リスト)
    submitted = st.form_submit_button("フィルター適用")

if "filtered" not in st.session_state or submitted:
    filtered_df = df.copy()
    if sel_tanto:
        filtered_df = filtered_df[filtered_df["担当者一覧"].apply(
            lambda x: any(t in str(x) for t in sel_tanto)
        )]
    if sel_front:
        filtered_df = filtered_df[filtered_df["フロント一覧"].apply(
            lambda x: any(f in str(x) for f in sel_front)
        )]
    if sel_client:
        filtered_df = filtered_df[filtered_df["client_name"].isin(sel_client)]
    if sel_focus:
        filtered_df = filtered_df[filtered_df["focus_level"].isin(sel_focus)]
    st.session_state["filtered"] = filtered_df
else:
    filtered_df = st.session_state["filtered"]

# --- リンクURL生成 ---
filtered_df["リンクURL"] = filtered_df["client_id"].apply(
    lambda cid: f"https://sho-san-client-ad-score.streamlit.app/Daily_Score?client_id={cid}"
)

st.divider()

# --- ヘッダー ---
header_cols = st.columns([2, 2, 2, 2, 1.5])
header_cols[0].markdown("**クライアント名**")
header_cols[1].markdown("**担当者**")
header_cols[2].markdown("**フロント**")
header_cols[3].markdown("**注力度**")
header_cols[4].markdown("**リンク**")

st.divider()

def vertical_center(content, height="70px"):
    safe_content = content if pd.notna(content) and str(content).strip() != "" else "&nbsp;"
    return f"""
    <div style="display: flex; align-items: center; height: {height}; min-height: {height};">
        {html.escape(safe_content) if safe_content != "&nbsp;" else safe_content}
    </div>
    """

for _, row in filtered_df.iterrows():
    cols = st.columns([2, 2, 2, 2, 1.5])
    row_height = "70px"
    row_style = f"border-bottom: 1px solid #ddd; height: {row_height}; min-height: {row_height}; display: flex; align-items: center;"
    with cols[0]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["client_name"])}</div>', unsafe_allow_html=True)
    with cols[1]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["担当者一覧"])}</div>', unsafe_allow_html=True)
    with cols[2]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["フロント一覧"])}</div>', unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f'<div style="{row_style}">{vertical_center(row["focus_level"])}</div>', unsafe_allow_html=True)
    with cols[4]:
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
