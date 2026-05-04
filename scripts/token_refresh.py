from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import boto3
import pyotp
import requests
from dotenv import load_dotenv, set_key
from kiteconnect import KiteConnect


LOGGER = logging.getLogger(__name__)

_ENV_PATH = Path(__file__).parent.parent / ".env"
_KITE_LOGIN_URL = "https://kite.zerodha.com/api/login"
_KITE_TWOFA_URL = "https://kite.zerodha.com/api/twofa"
_KITE_CONNECT_URL = "https://kite.zerodha.com/connect/login"
_S3_TOKEN_KEY = "secrets/kite_access_token.txt"


def _load_config() -> dict[str, str]:
    load_dotenv(_ENV_PATH)
    required = [
        "KITE_API_KEY",
        "KITE_API_SECRET",
        "KITE_USER_ID",
        "KITE_PASSWORD",
        "KITE_TOTP_SECRET",
        "AWS_REGION",
        "S3_BUCKET",
    ]
    config: dict[str, str] = {}
    missing = []
    for key in required:
        value = os.getenv(key, "").strip()
        if not value:
            missing.append(key)
        config[key] = value
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")
    return config


def _fetch_request_token(api_key: str, user_id: str, password: str, totp_secret: str) -> str:
    session = requests.Session()

    resp = session.post(_KITE_LOGIN_URL, data={"user_id": user_id, "password": password}, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Kite login failed: {body.get('message', body)}")
    request_id = body["data"]["request_id"]
    LOGGER.info("Kite login successful")

    totp_code = pyotp.TOTP(totp_secret).now()
    resp = session.post(
        _KITE_TWOFA_URL,
        data={
            "user_id": user_id,
            "request_id": request_id,
            "twofa_value": totp_code,
            "twofa_type": "totp",
            "skip_session": "",
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Kite TOTP verification failed: {body.get('message', body)}")
    LOGGER.info("Kite TOTP verified")

    connect_url = f"{_KITE_CONNECT_URL}?v=3&api_key={api_key}"
    resp = session.get(connect_url, allow_redirects=False, timeout=30)
    redirect_url = resp.headers.get("location", "")
    if not redirect_url:
        resp = session.get(connect_url, allow_redirects=True, timeout=30)
        redirect_url = resp.url

    params = parse_qs(urlparse(redirect_url).query)
    request_token = params.get("request_token", [None])[0]
    if not request_token:
        raise RuntimeError(f"request_token not found in redirect URL: {redirect_url}")
    LOGGER.info("Got request_token from Kite Connect")
    return request_token


def _exchange_for_access_token(api_key: str, api_secret: str, request_token: str) -> str:
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    return data["access_token"]


def _write_to_env(access_token: str) -> None:
    set_key(str(_ENV_PATH), "KITE_ACCESS_TOKEN", access_token, quote_mode="never")
    LOGGER.info("Written KITE_ACCESS_TOKEN to .env")


def _upload_to_s3(access_token: str, bucket: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)
    s3.put_object(
        Bucket=bucket,
        Key=_S3_TOKEN_KEY,
        Body=access_token.encode("utf-8"),
        ServerSideEncryption="AES256",
    )
    LOGGER.info("Uploaded token to s3://%s/%s", bucket, _S3_TOKEN_KEY)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    LOGGER.info("Starting Kite token refresh")
    try:
        config = _load_config()

        request_token = _fetch_request_token(
            api_key=config["KITE_API_KEY"],
            user_id=config["KITE_USER_ID"],
            password=config["KITE_PASSWORD"],
            totp_secret=config["KITE_TOTP_SECRET"],
        )
        access_token = _exchange_for_access_token(
            api_key=config["KITE_API_KEY"],
            api_secret=config["KITE_API_SECRET"],
            request_token=request_token,
        )
        _write_to_env(access_token)
        _upload_to_s3(access_token, config["S3_BUCKET"], config["AWS_REGION"])
        LOGGER.info("Token refresh complete")

    except Exception:
        LOGGER.exception("Token refresh failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
