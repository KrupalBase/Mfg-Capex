"""
Google OAuth 2.0 authentication for Flask apps.

Restricts access to @basepowercompany.com accounts.
Skipped entirely when GOOGLE_CLIENT_ID env var is not set (local dev).
"""
from __future__ import annotations

import os
import secrets
import time
from urllib.parse import urlencode

import requests as http_requests
from flask import Flask, redirect, request, session, render_template_string
from werkzeug.middleware.proxy_fix import ProxyFix

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
ALLOWED_DOMAIN = "basepowercompany.com"
AUTH_DEBUG = os.environ.get("AUTH_DEBUG", "").strip().lower() in {"1", "true", "yes", "y"}

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
SHEETS_READ_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
OAUTH_SCOPES = ["openid", "email", "profile", SHEETS_READ_SCOPE, CLOUD_PLATFORM_SCOPE]

LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Base Power - Mfg Budgeting</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#1A1A1A;color:#F0EEEB;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px}
.login-bg{position:fixed;inset:0;z-index:0;overflow:hidden;pointer-events:none}
.login-bg .glow{position:absolute;border-radius:50%;filter:blur(120px);opacity:.18}
.login-bg .glow.g1{width:420px;height:420px;background:#B2DD79;top:-80px;left:-60px}
.login-bg .glow.g2{width:320px;height:320px;background:#048EE5;bottom:-60px;right:-40px}
.login-wrap{position:relative;z-index:1;width:100%;max-width:400px}
.login-brand{text-align:center;margin-bottom:36px}
.login-brand .logo-mark{display:inline-flex;align-items:center;justify-content:center;width:56px;height:56px;border-radius:14px;background:linear-gradient(135deg,rgba(178,221,121,.2),rgba(178,221,121,.08));border:1px solid rgba(178,221,121,.25);margin-bottom:16px}
.login-brand .logo-mark svg{width:28px;height:28px}
.login-brand h1{font-size:22px;font-weight:700;color:#F0EEEB;letter-spacing:.3px}
.login-brand h1 span{color:#B2DD79}
.login-brand .tagline{font-size:12px;color:#9E9C98;margin-top:6px;letter-spacing:.4px;text-transform:uppercase;font-weight:600}
.login-card{background:#242422;border:1px solid #3E3D3A;border-radius:16px;padding:36px 32px;text-align:center;box-shadow:0 16px 48px rgba(0,0,0,.35)}
.login-card .title{font-size:18px;font-weight:700;margin-bottom:6px;color:#F0EEEB}
.login-card .subtitle{font-size:13px;color:#9E9C98;margin-bottom:28px;line-height:1.5}
.login-error{background:rgba(209,83,29,.12);border:1px solid rgba(209,83,29,.3);color:#D1531D;border-radius:8px;padding:10px 14px;font-size:12px;margin-bottom:20px;text-align:left;line-height:1.4}
.btn-google{display:inline-flex;align-items:center;justify-content:center;gap:12px;width:100%;background:#F0EEEB;color:#1A1A1A;border:none;border-radius:10px;padding:14px 24px;font-size:15px;font-weight:600;cursor:pointer;text-decoration:none;transition:all .2s;letter-spacing:.2px}
.btn-google:hover{background:#fff;box-shadow:0 4px 16px rgba(178,221,121,.2)}
.btn-google:active{transform:scale(.98)}
.btn-google svg{width:20px;height:20px;flex-shrink:0}
.login-footer{text-align:center;margin-top:28px;font-size:11px;color:#9E9C98;line-height:1.5}
.login-footer a{color:#B2DD79;text-decoration:none}
.login-footer a:hover{text-decoration:underline}
.login-security{display:flex;align-items:center;justify-content:center;gap:6px;margin-top:20px;font-size:11px;color:#9E9C98}
.login-security svg{width:14px;height:14px;color:#B2DD79;flex-shrink:0}
@media(max-width:480px){
    .login-card{padding:28px 22px;border-radius:14px}
    .login-brand .logo-mark{width:48px;height:48px;border-radius:12px}
    .login-brand .logo-mark svg{width:24px;height:24px}
    .login-brand h1{font-size:20px}
}
</style>
</head>
<body>
<div class="login-bg"><div class="glow g1"></div><div class="glow g2"></div></div>
<div class="login-wrap">
    <div class="login-brand">
        <div class="logo-mark">
            <svg viewBox="0 0 24 24" fill="none" stroke="#B2DD79" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
        </div>
        <h1><span>Base</span> Power</h1>
        <div class="tagline">Manufacturing Budgeting</div>
    </div>
    <div class="login-card">
        <div class="title">Welcome back</div>
        <div class="subtitle">Sign in with your Base Power Google account to access manufacturing CAPEX analytics.</div>
        {% if error %}<div class="login-error">{{ error }}</div>{% endif %}
        <a href="/auth/login" class="btn-google">
            <svg viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
            Sign in with Google
        </a>
        <div class="login-security">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
            Restricted to @basepowercompany.com accounts
        </div>
    </div>
    <div class="login-footer">
        <a href="https://www.basepowercompany.com" target="_blank">basepowercompany.com</a>
    </div>
</div>
</body>
</html>
"""


def _auth_enabled() -> bool:
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)


def _refresh_google_access_token(refresh_token: str) -> str | None:
    """Refresh user access token using stored Google refresh token."""
    token_resp = http_requests.post(GOOGLE_TOKEN_URL, data={
        "refresh_token": refresh_token,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }, timeout=10)
    if token_resp.status_code != 200:
        return None

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return None

    expires_in = int(token_data.get("expires_in", 3600) or 3600)
    session["google_access_token"] = access_token
    session["google_token_expiry"] = int(time.time()) + max(60, expires_in - 60)
    scopes_value = str(token_data.get("scope", "") or "").strip()
    if scopes_value:
        session["google_scopes"] = scopes_value
    new_refresh = token_data.get("refresh_token")
    if new_refresh:
        session["google_refresh_token"] = new_refresh
    return access_token


def get_google_access_token() -> str | None:
    """Get a valid Google user access token from session (refresh if needed)."""
    if not _auth_enabled():
        return None

    token = str(session.get("google_access_token", "") or "")
    expiry = int(session.get("google_token_expiry", 0) or 0)
    now = int(time.time())
    if token and expiry > (now + 30):
        return token

    refresh_token = str(session.get("google_refresh_token", "") or "")
    if refresh_token:
        return _refresh_google_access_token(refresh_token)
    return token or None


def init_auth(app: Flask) -> None:
    """Register auth routes and before_request hook on the Flask app."""
    if not _auth_enabled():
        return

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32).hex())

    @app.before_request
    def _require_login():
        if request.path.startswith("/auth/"):
            return None
        if not session.get("user_email"):
            return redirect("/auth/login-page")
        return None

    @app.route("/auth/login-page")
    def auth_login_page():
        error = request.args.get("error", "")
        return render_template_string(LOGIN_HTML, error=error)

    def _callback_url() -> str:
        """Build the OAuth callback URL, always HTTPS on Cloud Run."""
        proto = request.headers.get("X-Forwarded-Proto", request.scheme)
        host = request.headers.get("X-Forwarded-Host", request.host)
        if os.environ.get("K_SERVICE"):
            proto = "https"
        return f"{proto}://{host}/auth/callback"

    if AUTH_DEBUG:
        @app.route("/auth/debug")
        def auth_debug():
            """OAuth request debugging endpoint. Enable only with AUTH_DEBUG=true."""
            return {
                "request.scheme": request.scheme,
                "request.host": request.host,
                "request.url_root": request.url_root,
                "X-Forwarded-Proto": request.headers.get("X-Forwarded-Proto", "NOT SET"),
                "X-Forwarded-Host": request.headers.get("X-Forwarded-Host", "NOT SET"),
                "X-Forwarded-For": request.headers.get("X-Forwarded-For", "NOT SET"),
                "K_SERVICE": os.environ.get("K_SERVICE", "NOT SET"),
                "callback_url": _callback_url(),
            }

    @app.route("/auth/login")
    def auth_login():
        callback_url = _callback_url()
        state = secrets.token_urlsafe(24)
        session["oauth_state"] = state
        params = {
            "client_id": GOOGLE_CLIENT_ID,
            "redirect_uri": callback_url,
            "response_type": "code",
            "scope": " ".join(OAUTH_SCOPES),
            "hd": ALLOWED_DOMAIN,
            "prompt": "consent select_account",
            "access_type": "offline",
            "include_granted_scopes": "true",
            "state": state,
        }
        return redirect(GOOGLE_AUTH_URL + "?" + urlencode(params))

    @app.route("/auth/callback")
    def auth_callback():
        expected_state = str(session.pop("oauth_state", "") or "")
        incoming_state = str(request.args.get("state", "") or "")
        if not expected_state or incoming_state != expected_state:
            return redirect("/auth/login-page?error=Invalid+OAuth+state.+Please+try+again")

        code = request.args.get("code")
        if not code:
            return redirect("/auth/login-page?error=No+authorization+code+received")

        callback_url = _callback_url()
        token_resp = http_requests.post(GOOGLE_TOKEN_URL, data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": callback_url,
            "grant_type": "authorization_code",
        }, timeout=10)

        if token_resp.status_code != 200:
            return redirect("/auth/login-page?error=Failed+to+exchange+token")

        token_data = token_resp.json()
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = int(token_data.get("expires_in", 3600) or 3600)
        scopes_value = str(token_data.get("scope", "") or "").strip()
        if not access_token:
            return redirect("/auth/login-page?error=Failed+to+get+access+token")

        session["google_access_token"] = access_token
        if refresh_token:
            session["google_refresh_token"] = refresh_token
        if scopes_value:
            session["google_scopes"] = scopes_value
        session["google_token_expiry"] = int(time.time()) + max(60, expires_in - 60)
        user_resp = http_requests.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )

        if user_resp.status_code != 200:
            return redirect("/auth/login-page?error=Failed+to+get+user+info")

        user_info = user_resp.json()
        email: str = user_info.get("email", "")
        domain = email.split("@")[-1] if "@" in email else ""

        if domain != ALLOWED_DOMAIN:
            return redirect(
                "/auth/login-page?error="
                + f"Access+restricted+to+@{ALLOWED_DOMAIN}+accounts.+You+signed+in+as+{email}"
            )

        session["user_email"] = email
        session["user_name"] = user_info.get("name", email)
        session["user_picture"] = user_info.get("picture", "")
        return redirect("/")

    @app.route("/auth/logout")
    def auth_logout():
        session.clear()
        return redirect("/auth/login-page")
