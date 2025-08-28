# auth.py
import time
import hmac
import json
import base64
import hashlib
import streamlit as st

# ─────────────────────────────────
# 依存: streamlit-cookies-manager
# ─────────────────────────────────
try:
    from streamlit_cookies_manager import EncryptedCookieManager
except Exception:
    EncryptedCookieManager = None


# ============== 署名付きトークン ==============
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


# ============== Cookie ヘルパ ==============
def _get_cookie_manager():
    """
    Cookie マネージャを返す。ライブラリ未導入なら None を返し、
    その場合はセッションのみでフォールバック。
    """
    if EncryptedCookieManager is None:
        return None
    # keynames は内部的に暗号化用のキー名。値は任意文字列でOK
    cookies = EncryptedCookieManager(prefix="addrive", keynames=["addrive_cookie_key"])
    if not cookies.ready():
        # 初回ロード時に内部初期化のため 1 フレーム停止
        st.stop()
    return cookies


# ============== ログイン必須 ==============
def require_login():
    auth_cfg = st.secrets.get("auth", {})
    SHARED_EMAIL   = auth_cfg.get("shared_email", "")
    SHARED_PASSWORD= auth_cfg.get("shared_password", "")
    COOKIE_SECRET  = auth_cfg.get("cookie_secret", "")
    COOKIE_NAME    = auth_cfg.get("cookie_name", "addrive_token")
    COOKIE_DAYS    = int(auth_cfg.get("cookie_days", 30))

    if not (SHARED_EMAIL and SHARED_PASSWORD and COOKIE_SECRET):
        st.error("auth設定が不足しています（secrets.toml の [auth] を確認）")
        st.stop()

    cookies = _get_cookie_manager()

    # 1) Cookie / セッションに有効トークンがあれば通す
    token = cookies.get(COOKIE_NAME) if cookies is not None else st.session_state.get(COOKIE_NAME)
    if token:
        payload = _verify(token, COOKIE_SECRET)
        if payload:
            return  # 認証OK

    # 2) 未ログイン → ログインフォーム
    st.markdown("### 🔐 Ad Drive ログイン")
    st.info("※ログイン情報は社内のアイパス管理帳に記載されています。")

    if EncryptedCookieManager is None:
        st.warning("永続ログイン（Cookie保持）を行うには `streamlit-cookies-manager` の導入が必要です。現在はセッション(ブラウザを閉じるまで)のみ保持します。")

    with st.form("addrive_login"):
        email = st.text_input("メールアドレス", "")
        password = st.text_input("パスワード", type="password")
        remember = st.checkbox("ログイン状態を保持する（推奨）", value=True)
        submitted = st.form_submit_button("ログイン")

    if submitted:
        if email.strip() == SHARED_EMAIL and password == SHARED_PASSWORD:
            # 署名トークン作成（exp = n日後）
            exp = time.time() + COOKIE_DAYS * 24 * 60 * 60
            payload = {"u": "shared", "exp": exp}
            token = _sign(payload, COOKIE_SECRET)

            if cookies is not None and remember:
                # Cookie に保存（ブラウザ閉じても保持）
                cookies.set(
                    COOKIE_NAME, token,
                    max_age=COOKIE_DAYS * 24 * 60 * 60,  # 秒
                    same_site="Lax"  # 必要に応じて "Strict" / "None"
                )
                cookies.save()
            else:
                # フォールバック：セッションのみ
                st.session_state[COOKIE_NAME] = token

            st.success("ログインしました。")
            st.rerun()
        else:
            st.error("メールアドレスまたはパスワードが正しくありません。")

    st.stop()


# ============== ログアウト ==============
def logout():
    auth_cfg = st.secrets.get("auth", {})
    COOKIE_NAME = auth_cfg.get("cookie_name", "addrive_token")
    cookies = _get_cookie_manager()

    # Cookie / セッションの両方を削除
    if cookies is not None:
        cookies.delete(COOKIE_NAME)
        cookies.save()
    if COOKIE_NAME in st.session_state:
        del st.session_state[COOKIE_NAME]

    st.success("ログアウトしました。")
    st.rerun()
