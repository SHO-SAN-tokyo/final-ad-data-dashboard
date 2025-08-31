import streamlit as st
from google.cloud import bigquery

# 認証（全ページ共通の方式を踏襲）
from auth import require_login, logout
require_login()

st.set_page_config(page_title="⚙ My Settings", layout="wide")

# ---- ページタイトル
st.markdown("<h1>⚙ My Settings</h1>", unsafe_allow_html=True)
st.caption("ここでは Ad Drive のキャッシュ管理とログアウトができます。")

st.divider()

# ---- キャッシュクリア
st.subheader("🔄 キャッシュクリア")
st.write("数値が更新されない・表示が古い場合に実行してください。")
if st.button("🔄 キャッシュクリア"):
    st.cache_data.clear()
    st.session_state["data_version"] = st.session_state.get("data_version", 0) + 1
    st.rerun()


st.divider()

# ---- ログアウト
st.subheader("🚪 ログアウト")
st.write("共有アカウントの利用を停止し、ログイン画面へ戻ります。")
if st.button("ログアウトする", type="secondary", use_container_width=True):
    logout()

# （任意）ログイン情報の注意書き
with st.expander("ℹ️ 注意事項", expanded=False):
    st.markdown("""
- ログイン情報は**社内のアイパス管理帳**を参照してください。  
- Cookie によりログイン状態は維持されます（設定日数内）。共有端末ではご注意ください。
    """)
