import time
import hmac
import json
import base64
import hashlib
import streamlit as st

# 依存: streamlit-cookies-manager
try:
    from streamlit_cookies_manager import EncryptedCookieManager
except Exception as e:
    EncryptedCookieManager = None


def _b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _sign(payload: dict, secret: str) -> str:
    """HMAC-SHA256で署名付きトークン(JWT風)を生成"""
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    to_sign = f"{header_b64}.{payload_b64}".encode()
    sig = hmac.new(secret.encode(), to_sign, hashlib.sha256).digest()
    sig_b64 = _b64url_encode(sig)
    return f"{header_b64}.{payload_b64}.{sig_b64}"


def _verify(token: str, secret: str) -> dict | None:
    """署名/有効期限(exp)検証"""
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
        to_sign = f"{header_b64}.{payload_b64}".encode()
        expected = _b64url_encode(hmac.new(secret.encode(), to_sign, hashlib.sha256).digest())
        if not hmac.compare_digest(expected, sig_b64):
            return None
        payload = json.loads(_b64url_decode(payload_b64).decode())
        if "exp" in payload and time.time() > float(payload["exp"]):
            return None
        return payload
    except Exception:
        return None


def _get_cookie_manager():
    # Cookieマネージャ（あれば使用、無ければNone）
    if EncryptedCookieManager is None:
        return None
    keys = ["addrive_cookie_wrapper_key"]
    cookies = EncryptedCookieManager(prefix="addrive", password=None, keynames=keys)
    if not cookies.ready():
        st.stop()  # 初回ロードで内部初期化待ち
    return cookies


def require_login():
    auth_cfg = st.secrets.get("auth", {})
    SHARED_EMAIL = auth_cfg.get("shared_email", "")
    SHARED_PASSWORD = auth_cfg.get("shared_password", "")
    COOKIE_SECRET = auth_cfg.get("cookie_secret", "")
    COOKIE_NAME = auth_cfg.get("cookie_name", "addrive_token")
    COOKIE_DAYS = int(auth_cfg.get("cookie_days", 30))

    if not (SHARED_EMAIL and SHARED_PASSWORD and COOKIE_SECRET):
        st.error("auth設定が不足しています（secrets.toml の [auth] を確認）")
        st.stop()

    cookies = _get_cookie_manager()

    # 1) Cookieに有効トークンがあればそのまま通す
    token = None
    if cookies is not None:
        token = cookies.get(COOKIE_NAME)
    else:
        token = st.session_state.get(COOKIE_NAME)

    if token:
        payload = _verify(token, COOKIE_SECRET)
        if payload:
            return

    # 2) 未ログイン → ログインフォーム
    with st.container():
        st.markdown("### 🔐 Ad Drive ログイン")
        st.info("※ログイン情報は社内のアイパス管理帳に記載されています。")  # 👈 注意書きを追加

        with st.form("addrive_login"):
            email = st.text_input("メールアドレス", "")
            password = st.text_input("パスワード", type="password")
            remember = st.checkbox("ログイン状態を保持する（推奨）", value=True)
            submitted = st.form_submit_button("ログイン")

        if submitted:
            if email.strip() == SHARED_EMAIL and password == SHARED_PASSWORD:
                exp = time.time() + COOKIE_DAYS * 24 * 60 * 60
                payload = {"u": "shared", "exp": exp}
                token = _sign(payload, COOKIE_SECRET)

                if cookies is not None and remember:
                    cookies.set(COOKIE_NAME, token, max_age=COOKIE_DAYS * 24 * 60 * 60)
                    cookies.save()
                else:
                    st.session_state[COOKIE_NAME] = token

                st.success("ログインしました。")
                st.experimental_rerun()
            else:
                st.error("メールアドレスまたはパスワードが正しくありません。")

        st.stop()


def logout():
    """任意の場所で呼べるログアウト関数"""
    auth_cfg = st.secrets.get("auth", {})
    COOKIE_NAME = auth_cfg.get("cookie_name", "addrive_token")
    cookies = _get_cookie_manager()
    if cookies is not None:
        cookies.delete(COOKIE_NAME)
        cookies.save()
    if COOKIE_NAME in st.session_state:
        del st.session_state[COOKIE_NAME]
    st.success("ログアウトしました。")
    st.experimental_rerun()
