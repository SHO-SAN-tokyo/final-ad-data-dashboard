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

with st.expander("🛠️ 広告数値の手動更新（管理者用・通常は触らないでOK）", expanded=False):
    st.warning("※ この操作は数分かかる場合あり、同時に何度も押さないでください。")
    URL_META = "https://asia-northeast1-careful-chess-406412.cloudfunctions.net/upload-sql-data"
    URL_GOOGLE = "https://asia-northeast1-careful-chess-406412.cloudfunctions.net/upload-sql-data-pmax"
    st.markdown(f"**Meta広告の数値を更新:**  \n[こちらをクリック（所要時間：1~2分）]({URL_META})", unsafe_allow_html=True)
    st.markdown(f"**Google広告の数値を更新:**  \n[こちらをクリック（所要時間：1分弱）]({URL_GOOGLE})", unsafe_allow_html=True)
    st.info("クリック後、画面に完了ログが表示されたら、一呼吸おいてキャッシュクリアボタンでを押して最新化してください。")
