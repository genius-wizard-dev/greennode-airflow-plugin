"""VNG Cloud / GreenNode Airflow Hook."""

import base64
import os
import time
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.sdk.bases.hook import BaseHook

DEFAULT_IAM_HOST = "https://dev-iam-proxy.dataplatform.vngcloud.tech"
DEFAULT_TOKEN_PATH = "/accounts-api/v2/auth/token"
DEFAULT_DATA_PLATFORM_URL = "https://dev-backend-proxy.dataplatform.vngcloud.tech/"
DEFAULT_FE_URL = "https://dev-app.dataplatform.vngcloud.tech"

DEFAULT_TOKEN_URL = f"{DEFAULT_IAM_HOST}{DEFAULT_TOKEN_PATH}"

_DEFAULT_TIMEOUT = 30
_LOG_BODY_LIMIT = 1000


class VNGCloudHook(BaseHook):
    """Hook for VNG Cloud IAM (access token) and Data Platform API."""

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
        self.fe_url = cfg["fe_url"].rstrip("/")
        self._client_id = cfg["client_id"]
        self._client_secret = cfg["client_secret"]

    def _load_connection_config(self) -> dict[str, Any]:
        """Load config with priority: Connection → Variable → env var."""
        try:
            conn = self.get_connection(self.vng_conn_id)
        except Exception:
            conn = None

        client_id: str | None = None
        client_secret: str | None = None
        iam_host = DEFAULT_IAM_HOST
        token_path = DEFAULT_TOKEN_PATH
        data_platform_url = DEFAULT_DATA_PLATFORM_URL
        fe_url = DEFAULT_FE_URL

        if conn is not None:
            client_id = conn.login
            client_secret = conn.password
            if conn.host:
                iam_host = conn.host.rstrip("/")
            extra = conn.extra_dejson or {}
            token_path = extra.get("token_path", token_path)
            data_platform_url = extra.get("data_platform_url", data_platform_url)
            fe_url = extra.get("fe_url", fe_url)

        if not client_id or not client_secret:
            from airflow.models import Variable

            client_id = client_id or Variable.get("vng_client_id", default_var=None)
            client_secret = client_secret or Variable.get("vng_client_secret", default_var=None)
            iam_host = Variable.get("vng_iam_host", default_var=iam_host).rstrip("/")
            token_path = Variable.get("vng_token_path", default_var=token_path)
            data_platform_url = Variable.get("vng_data_platform_url", default_var=data_platform_url)
            fe_url = Variable.get("vng_fe_url", default_var=fe_url)

        client_id = client_id or os.getenv("VNG_CLIENT_ID")
        client_secret = client_secret or os.getenv("VNG_CLIENT_SECRET")

        if not client_id or not client_secret:
            raise AirflowException(
                f"VNG Cloud credentials are not configured. Set them via one of:\n"
                f"  1. Airflow Connection '{self.vng_conn_id}' (login=client_id, password=client_secret)\n"
                f"  2. Airflow Variables: 'vng_client_id', 'vng_client_secret'\n"
                f"  3. Env vars: VNG_CLIENT_ID, VNG_CLIENT_SECRET"
            )

        if not token_path.startswith("/"):
            token_path = "/" + token_path
        token_url = f"{iam_host}{token_path}"

        return {
            "client_id": client_id,
            "client_secret": client_secret,
            "token_url": token_url,
            "data_platform_url": data_platform_url,
            "fe_url": fe_url,
        }

    def get_job_ui_url(self, workspace_id: str, job_id: str) -> str:
        """Build link to Spark Job page on Data Platform UI."""
        return f"{self.fe_url}/workspaces/{workspace_id}/jobs/{job_id}"

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: int = _DEFAULT_TIMEOUT,
        op: str = "request",
    ) -> dict[str, Any]:
        """HTTP request wrapper with consistent error handling and logging."""
        try:
            resp = requests.request(
                method, url, headers=headers, json=json_body, timeout=timeout
            )
        except requests.exceptions.Timeout as e:
            raise AirflowException(
                f"VNG {op} timed out after {timeout}s: {url}"
            ) from e
        except requests.exceptions.ConnectionError as e:
            raise AirflowException(
                f"VNG {op} connection error to {url}: {e}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise AirflowException(
                f"VNG {op} request failed for {url}: {e}"
            ) from e

        if not resp.ok:
            body_preview = (resp.text or "")[:_LOG_BODY_LIMIT]
            self.log.error(
                "VNG %s failed: %s %s — url=%s body=%s",
                op, resp.status_code, resp.reason, url, body_preview,
            )
            raise AirflowException(
                f"VNG {op} returned HTTP {resp.status_code} {resp.reason} "
                f"for {url}. Response: {body_preview}"
            )

        try:
            return resp.json()
        except ValueError as e:
            raise AirflowException(
                f"VNG {op} returned non-JSON response from {url}: "
                f"{(resp.text or '')[:_LOG_BODY_LIMIT]}"
            ) from e

    def get_access_token(self, force_refresh: bool = False) -> str:
        """Get access token (in-memory cache, auto-refresh 60s before expiry)."""
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

        data = self._request(
            "POST",
            self.token_url,
            headers=headers,
            json_body=payload,
            op="token request",
        )

        token = data.get("access_token")
        if not token:
            raise AirflowException(
                f"VNG token response missing 'access_token' field: {data}"
            )

        self._token = token
        self._expiry = now + int(data.get("expires_in", 1800))
        self.log.info(
            "VNG access token renewed (expires_in=%ss)", data.get("expires_in")
        )
        return self._token

    def get_headers(self) -> dict[str, str]:
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _unwrap(self, resp_json: dict[str, Any], op: str) -> dict[str, Any]:
        if not resp_json.get("success", True):
            error = (
                resp_json.get("error")
                or resp_json.get("message")
                or "unknown error"
            )
            raise AirflowException(f"VNG Data Platform {op} error: {error}")
        return resp_json.get("data") or {}

    def submit_job_run(
        self, workspace_id: str, job_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        url = (
            f"{self.data_platform_url}/api/v1/workspaces/{workspace_id}"
            f"/spark-jobs/{job_id}/runs"
        )
        data = self._request(
            "POST", url, headers=self.get_headers(), json_body=payload,
            timeout=60, op="submit job run",
        )
        return self._unwrap(data, "submit job run")

    def get_job_run(
        self, workspace_id: str, job_id: str, run_id: str
    ) -> dict[str, Any]:
        url = (
            f"{self.data_platform_url}/api/v1/workspaces/{workspace_id}"
            f"/spark-jobs/{job_id}/runs/{run_id}"
        )
        data = self._request(
            "GET", url, headers=self.get_headers(), op="get job run",
        )
        return self._unwrap(data, "get job run")

    def cancel_job_run(
        self, workspace_id: str, job_id: str, run_id: str
    ) -> dict[str, Any]:
        url = (
            f"{self.data_platform_url}/api/v1/workspaces/{workspace_id}"
            f"/spark-jobs/{job_id}/runs/{run_id}/cancel"
        )
        data = self._request(
            "POST", url, headers=self.get_headers(), op="cancel job run",
        )
        return self._unwrap(data, "cancel job run")
