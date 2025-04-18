import streamlit as st
from streamlit_option_menu import option_menu
import importlib

st.set_page_config(page_title="Ad Drive", layout="wide")

# 🔸 表示名とモジュール名のマッピング（絵文字はなくてもOK）
PAGES = {
    "ホーム": "pages.home",
    "メインダッシュボード": "pages.Main_Dashboard",
    "ユニット設定": "pages.Unit_Setting"
}

# 🔹 サイドバーのメニュー
with st.sidebar:
    selected = option_menu(
        menu_title="🫧Ad Drive🫧",
        options=list(PAGES.keys()),
        icons=["house", "bar-chart", "gear"],
        menu_icon="cast",
        default_index=0
    )

# 🔁 ページに応じて動的にインポートして実行
module = importlib.import_module(PAGES[selected])
module.render()
