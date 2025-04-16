import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME - ダッシュボード入口")

st.markdown("### ページ一覧")

# ✅ これは Python 関数として呼び出す
st.page_link("1_Main_Dashboard", label="📊 メインダッシュボードへ")
st.page_link("2_Unit_Setting", label="⚙️ Unit設定ページへ")
