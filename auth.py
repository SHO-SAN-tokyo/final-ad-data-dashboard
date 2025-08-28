import time
import hmac
import json
import base64
import hashlib
import streamlit as st

# 依存: streamlit-cookies-manager（無くてもセッション維持で動くフォールバックあり）
try:
    from streamlit_cookies_manager import EncryptedCookieManager
except Exception:
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
    """Cookie マネージャの初期化（失敗時は None を返す=セッション維持にフォールバック）"""
    if EncryptedCookieManager is None:
        return None

    auth_cfg = st.secrets.get("auth", {})
    # cookie_password は secrets.toml で管理してください。無ければ最終手段として cookie_secret を流用
    cookie_password = auth_cfg.get("cookie_password") or auth_cfg.get("cookie_secret")

    if not cookie_password:
        # パスワードが無いとライブラリが初期化できないのでフォールバック
        return None

    try:
        keys = ["addrive_cookie_wrapper_key"]
        cookies = EncryptedCookieManager(
            prefix="addrive",
            password=str(cookie_password),  # None を渡さない
            keynames=keys,
        )
        if not cookies.ready():
            # 初回ロードで内部初期化が必要。ここで止めて再実行を待つ
            st.stop()
        return cookies
    except Exception:
        # 何らかの理由で初期化失敗→フォールバック
        return None


def require_login():
    auth_cfg = st.secrets.get("auth", {})
    SHARED_EMAIL = auth_cfg.get("shared_email", "")
    SHARED_PASSWORD = auth_cfg.get("shared_password", "")
    COOKIE_SECRET = auth_cfg.get("cookie_secret", "")
    COOKIE_NAME = auth_cfg.get("cookie_name", "addrive_token")
    COOKIE_DAYS = int(auth_cfg.get("cookie_days", 30))

    # 最低限必要な値
    if not (SHARED_EMAIL and SHARED_PASSWORD and COOKIE_SECRET):
        st.error("auth設定が不足しています（secrets.toml の [auth] を確認）")
        st.stop()

    cookies = _get_cookie_manager()

    # 1) Cookie/セッションに有効トークンがあれば認証OK
    token = cookies.get(COOKIE_NAME) if cookies is not None else st.session_state.get(COOKIE_NAME)
    if token:
        payload = _verify(token, COOKIE_SECRET)
        if payload:
            return  # 認証OK

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
                    # Cookie（永続）に保存
                    cookies.set(COOKIE_NAME, token, max_age=COOKIE_DAYS * 24 * 60 * 60)
                    cookies.save()
                else:
                    # ライブラリ未導入/remember=False の場合はセッションのみ（ブラウザ閉じると消える）
                    st.session_state[COOKIE_NAME] = token

                st.success("ログインしました。")
                st.experimental_rerun()
            else:
                st.error("メールアドレスまたはパスワードが正しくありません。")

        # フォーム表示中はページ描画を止める
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
