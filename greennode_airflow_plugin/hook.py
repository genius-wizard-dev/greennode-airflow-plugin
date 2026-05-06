import os
import time
import base64
import requests
from airflow.hooks.base import BaseHook

# NOTE: typing.Optional chưa được sử dụng trong module này,
# giữ lại để tham khảo nếu sau này cần type hint cho các phương thức.
# from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_TOKEN_URL = "https://pub-iamapis.api-dev.vngcloud.tech/accounts-api/v2/auth/token"
DEFAULT_DATA_PLATFORM_URL = "https://dataplatform.api-dev.vngcloud.tech"


class VNGCloudHook(BaseHook):
    """
    Hook để kết nối với VNG Cloud IAM và lấy Access Token.
    """

    conn_name_attr = "vng_conn_id"
    default_conn_name = "vng_iam"

    def __init__(self, vng_conn_id: str | None = None, token_url: str | None = None):
        super().__init__()
        self.vng_conn_id = vng_conn_id or self.default_conn_name
        self.token_url = token_url or DEFAULT_TOKEN_URL
        self._token = None
        self._expiry = 0

    def _get_credentials(self):
        try:
            conn = self.get_connection(self.vng_conn_id)
            return conn.login, conn.password
        except Exception:
            client_id = os.getenv("VNG_CLIENT_ID")
            client_secret = os.getenv("VNG_CLIENT_SECRET")
            if not client_id or not client_secret:
                raise ValueError("VNG_CLIENT_ID và VNG_CLIENT_SECRET chưa được cấu hình!")
            return client_id, client_secret

    def get_access_token(self, force_refresh: bool = False) -> str:
        now = int(time.time())

        if not force_refresh and self._token and now < self._expiry - 60:
            return self._token

        client_id, client_secret = self._get_credentials()
        credentials = f"{client_id}:{client_secret}"
        basic_auth = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        headers = {
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/json",
        }
        payload = {"grant_type": "client_credentials", "scope": "email"}

        response = requests.post(self.token_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        self._token = data["access_token"]
        self._expiry = now + data.get("expires_in", 1800)

        self.log.info(f"VNG Access Token renewed. Expires in {data.get('expires_in')}s")
        return self._token

    def get_headers(self) -> dict[str, str]:
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
