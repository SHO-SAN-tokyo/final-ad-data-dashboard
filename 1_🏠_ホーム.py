import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME - ダッシュボード入口")

st.markdown("### 🔗 ページ一覧")

st.page_link("pages/2_🫧_メインダッシュボード.py", label="🫧 Ad Drive")
st.page_link("pages/3_⚙️_ユニット設定.py", label="⚙️ Unit設定")
