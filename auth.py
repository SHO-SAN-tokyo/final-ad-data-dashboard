import streamlit as st

def require_login():
    users = st.secrets["auth"].to_dict()
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        st.title("🔒 ログインが必要です")
        with st.form("login_form"):
            username = st.text_input("ユーザー名")
            password = st.text_input("パスワード", type="password")
            submitted = st.form_submit_button("ログイン")
            if submitted:
                if username in users and users[username] == password:
                    st.session_state["authenticated"] = True
                    st.success("ログイン成功！")
                    st.rerun()
                else:
                    st.error("ユーザー名またはパスワードが違います")
        st.stop()
