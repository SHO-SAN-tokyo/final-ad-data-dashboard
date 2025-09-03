import time
import hmac
import json
import base64
import hashlib
import streamlit as st

# 依存: streamlit-cookies-manager
try:
    from streamlit_cookies_manager import EncryptedCookieManager
except Exception:
    EncryptedCookieManager = None

# ─────────────────────────────────────────
# util
# ─────────────────────────────────────────
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

# ─────────────────────────────────────────
# Cookie manager singleton
# ─────────────────────────────────────────
_COOKIES_SINGLETON_KEY = "_addrive_cookie_mgr"

def _get_cookie_manager(password: str | None):
    """
    アプリ内で1つだけ CookieManager を使う。
    - 既に作成済みならそれを返す
    - 未作成なら作って保存
    """
    if EncryptedCookieManager is None:
        return None

    # 既に作成済みなら再利用
    if _COOKIES_SINGLETON_KEY in st.session_state:
        mgr = st.session_state[_COOKIES_SINGLETON_KEY]
    else:
        mgr = EncryptedCookieManager(prefix="addrive", password=password)
        st.session_state[_COOKIES_SINGLETON_KEY] = mgr

    # 初回ロード時の内部初期化待ち
    if not mgr.ready():
        st.stop()
    return mgr

# ─────────────────────────────────────────
# public API
# ─────────────────────────────────────────
def require_login():
    auth_cfg = st.secrets.get("auth", {})
    SHARED_EMAIL   = auth_cfg.get("shared_email", "")
    SHARED_PASSWORD = auth_cfg.get("shared_password", "")
    COOKIE_SECRET  = auth_cfg.get("cookie_secret", "")
    COOKIE_PASSWORD = auth_cfg.get("cookie_password", "")  # 暗号化用
    COOKIE_NAME    = auth_cfg.get("cookie_name", "addrive_token")
    COOKIE_DAYS    = int(auth_cfg.get("cookie_days", 30))

    if not (SHARED_EMAIL and SHARED_PASSWORD and COOKIE_SECRET):
        st.error("auth設定が不足しています（secrets.toml の [auth] を確認）")
        st.stop()

    cookies = _get_cookie_manager(COOKIE_PASSWORD)

    # 1) Cookieに有効トークンがあれば通す
    token = cookies.get(COOKIE_NAME) if cookies is not None else st.session_state.get(COOKIE_NAME)
    if token:
        payload = _verify(token, COOKIE_SECRET)
        if payload:
            return

    # 2) 未ログイン → ログインフォーム
    with st.container():
        st.markdown("### 🔐 Ad Drive ログイン")
        st.info("※ログイン情報は社内のアイパス管理帳に記載されています。")

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
                st.rerun()
            else:
                st.error("メールアドレスまたはパスワードが正しくありません。")

        st.stop()

def logout():
    """任意の場所で呼べるログアウト関数（My Settings から呼び出し）"""
    auth_cfg = st.secrets.get("auth", {})
    COOKIE_NAME     = auth_cfg.get("cookie_name", "addrive_token")
    COOKIE_PASSWORD = auth_cfg.get("cookie_password", "")

    cookies = _get_cookie_manager(COOKIE_PASSWORD)  # 既存インスタンスを再利用
    if cookies is not None:
        cookies.delete(COOKIE_NAME)
        cookies.save()
    if COOKIE_NAME in st.session_state:
        del st.session_state[COOKIE_NAME]

    st.success("ログアウトしました。")
    st.rerun()
