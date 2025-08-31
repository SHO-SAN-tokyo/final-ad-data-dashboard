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

# ---- CSS 共通スタイル（キャッシュクリア & ログアウトを統一）
st.markdown("""
<style>
/* ボタンのサイズ感を統一（9pxフォント・同じ高さ/パディング） */
div.stButton > button {
    font-size: 13px !important;
    padding: 0.4em 1.2em !important;
    height: auto !important;
    min-height: 35px !important;
}
</style>
""", unsafe_allow_html=True)

# ---- キャッシュクリア
st.subheader("🔄 キャッシュクリア")
st.write("数値が更新されない・表示が古い場合に実行してください。")

if st.button("🔄 キャッシュクリア", key="clear_cache_btn"):
    st.cache_data.clear()
    st.session_state["cache_cleared"] = True   # ← フラグを立てる
    st.rerun()

# ✅ 成功メッセージを表示（ボタンの下）
if st.session_state.get("cache_cleared"):
    st.success("✅ キャッシュクリア完了しました！")
    # 一度表示したらフラグを消す（何度も出ないように）
    del st.session_state["cache_cleared"]


st.divider()

# ---- ログアウト
st.subheader("🚪 ログアウト")
st.write("ログイン画面へ戻ります。")
if st.button("🚪 ログアウト", key="logout_btn"):
    logout()

# （任意）ログイン情報の注意書き
with st.expander("ℹ️ ログインについて注意事項", expanded=False):
    st.markdown("""
- ログイン情報は**社内のアイパス管理帳**を参照してください。  
- Cookie によりログイン状態は維持されます（設定日数内）。共有端末ではご注意ください。
    """)

st.divider()

# ---- 広告数値の強制更新
st.subheader("🕒 広告数値の強制更新")
st.write("Meta/Googleからの広告数値は毎日0時･6時･12時･18時に自動更新されますが、今すぐ強制更新して数値を反映したいときのみ以下から手順通りに行ってください。")
st.write("⚠️CVリストの更新ではありません。CVリストは「ダッシュボードに送る」からデータ反映できます。")
with st.expander("🛠️ 広告数値の手動更新（管理者用・通常は触らないでOK）", expanded=False):
    st.warning("※ この操作は数分かかる場合あり、同時に何度も押さないでください。")
    URL_META = "https://asia-northeast1-careful-chess-406412.cloudfunctions.net/upload-sql-data"
    URL_GOOGLE = "https://asia-northeast1-careful-chess-406412.cloudfunctions.net/upload-sql-data-pmax"
    st.markdown(f"**Meta広告の数値を更新:**  \n[こちらをクリック（所要時間：1~2分）]({URL_META})", unsafe_allow_html=True)
    st.markdown(f"**Google広告の数値を更新:**  \n[こちらをクリック（所要時間：1分弱）]({URL_GOOGLE})", unsafe_allow_html=True)
    st.info("クリック後、画面に完了ログが表示されたら、一呼吸おいてキャッシュクリアボタンを押して最新化してください。")
