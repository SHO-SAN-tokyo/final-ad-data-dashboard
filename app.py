# app.py
from streamlit_option_menu import option_menu
import streamlit as st

st.set_page_config(page_title="Ad Drive", layout="wide")

# 🌟 サイドバーのメニュー
with st.sidebar:
    selected = option_menu(
        menu_title="📑 メニュー",
        options=["🏠 ホーム", "📊 メインダッシュボード", "ユニット設定"],
        icons=["house", "bar-chart", "gear"],
        menu_icon="cast",
        default_index=0
    )

# 🔁 ページの切り替えロジック
if selected == "🏠 ホーム":
    st.title("【HOME】🌈 Ad Drive")
    st.markdown("🔗 **ページ一覧**\n- 📊 メインダッシュボード\n- ⚙️ ユニット設定")

elif selected == "📊 メインダッシュボード":
    st.title("📊 メインダッシュボード")
    st.write("ここにダッシュボードの内容を表示")

elif selected == "⚙️ ユニット設定":
    st.title("⚙️ ユニット設定")
    st.write("ここにユニット設定フォームなどを表示")

