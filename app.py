import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME - ダッシュボード入口")

st.markdown("### 🔗 ページ一覧")

# ✅ ページタイトルでリンク
st.page_link("メインダッシュボード", label="📊 メインダッシュボードへ")
st.page_link("Unit設定", label="⚙️ Unit設定ページへ")
