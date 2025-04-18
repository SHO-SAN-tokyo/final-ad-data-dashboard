import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("【HOME】🫧 Ad Drive🫧")

st.markdown("### 🔗 ページ一覧")

st.page_link("pages/1_Main_Dashboard.py", label="🫧 Ad Drive")
st.page_link("pages/2_Unit_Setting.py", label="⚙️ Unit設定")
