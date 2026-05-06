"""VNG Cloud / GreenNode Airflow Hook."""

import base64
import json
import os
import time
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

DEFAULT_IAM_HOST = "https://pub-iamapis.api-dev.vngcloud.tech"
DEFAULT_TOKEN_PATH = "/accounts-api/v2/auth/token"
DEFAULT_DATA_PLATFORM_URL = "https://dataplatform.api-dev.vngcloud.tech"

DEFAULT_TOKEN_URL = f"{DEFAULT_IAM_HOST}{DEFAULT_TOKEN_PATH}"


class VNGCloudHook(BaseHook):
    """Hook để kết nối với VNG Cloud IAM (lấy access token) và Data Platform API."""

    conn_name_attr = "vng_conn_id"
    default_conn_name = "vng_cloud_default"
    conn_type = "generic"
    hook_name = "VNG Cloud"

    def __init__(
        self,
        vng_conn_id: str | None = None,
        token_url: str | None = None,
        data_platform_url: str | None = None,
    ):
        super().__init__()
        self.vng_conn_id = vng_conn_id or self.default_conn_name
        self._token: str | None = None
        self._expiry: int = 0

        cfg = self._load_connection_config()
        self.token_url = token_url or cfg["token_url"]
        self.data_platform_url = (data_platform_url or cfg["data_platform_url"]).rstrip("/")
        self._client_id = cfg["client_id"]
        self._client_secret = cfg["client_secret"]

    def _load_connection_config(self) -> dict[str, Any]:
        """Đọc config từ Airflow Connection; fallback sang env nếu connection không tồn tại."""
        try:
            conn = self.get_connection(self.vng_conn_id)
        except Exception:
            conn = None

        client_id: str | None = None
        client_secret: str | None = None
        iam_host = DEFAULT_IAM_HOST
        token_path = DEFAULT_TOKEN_PATH
        data_platform_url = DEFAULT_DATA_PLATFORM_URL

        if conn is not None:
            client_id = conn.login
            client_secret = conn.password
            if conn.host:
                iam_host = conn.host.rstrip("/")
            extra = conn.extra_dejson or {}
            token_path = extra.get("token_path", token_path)
            data_platform_url = extra.get("data_platform_url", data_platform_url)

        client_id = client_id or os.getenv("VNG_CLIENT_ID")
        client_secret = client_secret or os.getenv("VNG_CLIENT_SECRET")

        if not client_id or not client_secret:
            raise AirflowException(
                f"VNG Cloud credentials không được cấu hình. "
                f"Tạo Airflow Connection '{self.vng_conn_id}' với login=client_id, password=client_secret, "
                f"hoặc set env VNG_CLIENT_ID / VNG_CLIENT_SECRET."
            )

        if not token_path.startswith("/"):
            token_path = "/" + token_path
        token_url = f"{iam_host}{token_path}"

        return {
            "client_id": client_id,
            "client_secret": client_secret,
            "token_url": token_url,
            "data_platform_url": data_platform_url,
        }

    def get_access_token(self, force_refresh: bool = False) -> str:
        """Lấy access token (cache trong memory, tự refresh trước khi hết hạn 60s)."""
        now = int(time.time())
        if not force_refresh and self._token and now < self._expiry - 60:
            return self._token

        credentials = f"{self._client_id}:{self._client_secret}"
        basic_auth = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        headers = {
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/json",
        }
        payload = {"grant_type": "client_credentials", "scope": "email"}

        resp = requests.post(self.token_url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        self._token = data["access_token"]
        self._expiry = now + data.get("expires_in", 1800)
        self.log.info("VNG Access Token renewed (expires_in=%ss)", data.get("expires_in"))
        return self._token

    def get_headers(self) -> dict[str, str]:
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _unwrap(self, resp_json: dict[str, Any]) -> dict[str, Any]:
        if not resp_json.get("success", True):
            error = resp_json.get("error") or resp_json.get("message") or "Unknown error"
            raise AirflowException(f"Data Platform API error: {error}")
        return resp_json.get("data") or {}

    def submit_job_run(self, workspace_id: str, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.data_platform_url}/api/v1/workspaces/{workspace_id}/spark-jobs/{job_id}/runs"
        resp = requests.post(url, headers=self.get_headers(), json=payload, timeout=60)
        resp.raise_for_status()
        return self._unwrap(resp.json())

    def get_job_run(self, workspace_id: str, job_id: str, run_id: str) -> dict[str, Any]:
        url = f"{self.data_platform_url}/api/v1/workspaces/{workspace_id}/spark-jobs/{job_id}/runs/{run_id}"
        resp = requests.get(url, headers=self.get_headers(), timeout=30)
        resp.raise_for_status()
        return self._unwrap(resp.json())

    def cancel_job_run(self, workspace_id: str, job_id: str, run_id: str) -> dict[str, Any]:
        url = f"{self.data_platform_url}/api/v1/workspaces/{workspace_id}/spark-jobs/{job_id}/runs/{run_id}/cancel"
        resp = requests.post(url, headers=self.get_headers(), timeout=30)
        resp.raise_for_status()
        return self._unwrap(resp.json())
