import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME - ダッシュボード入口")

st.markdown("### 🔗 ページ一覧")

st.page_link("pages/01_🫧Ad_Drive.py", label="🫧 Ad Drive")
st.page_link("pages/02_🧩SHO-SAN_market.py", label="🧩 SHO-SAN market")
st.page_link("pages/07_⚙️Unit_Settings.py", label="⚙️ Unit設定")



