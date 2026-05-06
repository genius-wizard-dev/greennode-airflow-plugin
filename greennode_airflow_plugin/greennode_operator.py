import time
from typing import Any
import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from greennode_airflow_plugin.hook import VNGCloudHook, DEFAULT_DATA_PLATFORM_URL, DEFAULT_TOKEN_URL


class GreenNodeOperator(BaseOperator):
    """
    GreenNodeOperator - Trigger Spark Job thông qua Data Platform API và poll trạng thái.

    API flow:
      1. POST /api/v1/workspaces/{ws}/spark-jobs/{job}/runs              →  trigger job run
      2. GET  /api/v1/workspaces/{ws}/spark-jobs/{job}/runs/{runId}      →  poll status
      3. POST /api/v1/workspaces/{ws}/spark-jobs/{job}/runs/{runId}/cancel  →  cancel (on kill)
    """

    # ── Trạng thái theo Data Platform API schema ──
    # Chưa kết thúc → tiếp tục poll
    _PENDING_STATES = {"QUEUING", "SCHEDULING", "PENDING", "RUNNING"}

    # Kết thúc thành công
    _SUCCESS_STATES = {"SUCCESS"}

    # Kết thúc thất bại
    _FAILURE_STATES = {"FAILED", "CANCELLED"}

    def __init__(
        self,
        workspace_id: str,
        job_id: str,
        application_args: list[str] | None = None,
        data_platform_url: str | None = None,
        vng_conn_id: str | None = None,
        token_url: str | None = None,
        polling_period_seconds: int = 15,
        do_xcom_push: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.workspace_id = workspace_id
        self.job_id = job_id
        self.application_args = application_args or [""]
        self.data_platform_url = (data_platform_url or DEFAULT_DATA_PLATFORM_URL).rstrip("/")
        self.vng_conn_id = vng_conn_id
        self.token_url = token_url or DEFAULT_TOKEN_URL
        self.polling_period_seconds = polling_period_seconds
        self.do_xcom_push = do_xcom_push

        # NOTE: config_override không dùng nữa vì Data Platform API dùng application_args.
        # self.config_override = config_override or {}

        # Internal state for on_kill cancel
        self._run_id: str | None = None
        self._cancel_url: str | None = None
        self._headers: dict[str, str] | None = None

    # ── helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _unwrap_data(resp_json: dict[str, Any]) -> dict[str, Any]:
        """
        Data Platform API wraps tất cả response trong envelope:
            {"success": true, "data": {...}, "error": null, "message": "..."}
        Trả về phần `data`. Nếu success=false hoặc không có data thì raise.
        """
        if not resp_json.get("success", True):
            error_msg = resp_json.get("error") or resp_json.get("message") or "Unknown error"
            raise AirflowException(f"Data Platform API error: {error_msg}")
        return resp_json.get("data") or {}

    # ── execute ──────────────────────────────────────────────────────────

    def execute(self, context):
        hook = VNGCloudHook(vng_conn_id=self.vng_conn_id, token_url=self.token_url)
        self._headers = hook.get_headers()

        base = self.data_platform_url
        ws, jid = self.workspace_id, self.job_id
        trigger_url = f"{base}/api/v1/workspaces/{ws}/spark-jobs/{jid}/runs"
        payload = {"application_args": self.application_args}

        self.log.info(f"Triggering Spark Job [{jid}] in workspace [{ws}]")
        self.log.debug(f"POST {trigger_url} with payload: {payload}")

        # ── Step 1: Trigger job run ──
        trigger_resp = requests.post(trigger_url, headers=self._headers, json=payload, timeout=60)
        trigger_resp.raise_for_status()
        data = self._unwrap_data(trigger_resp.json())

        self._run_id = data.get("id") or data.get("run_id")
        if not self._run_id:
            raise AirflowException(f"Không tìm thấy run id trong response: {data}")

        self.log.info(f"Job triggered successfully. run_id = {self._run_id}")

        # Chuẩn bị URL cho cancel (on_kill)
        self._cancel_url = f"{base}/api/v1/workspaces/{ws}/spark-jobs/{jid}/runs/{self._run_id}/cancel"

        # ── XCom push ──
        if self.do_xcom_push:
            self.xcom_push(key="workspace_id", value=ws)
            self.xcom_push(key="job_id", value=jid)
            self.xcom_push(key="run_id", value=self._run_id)

        # ── Step 2: Poll job status ──
        status_url = f"{base}/api/v1/workspaces/{ws}/spark-jobs/{jid}/runs/{self._run_id}"
        self._poll_job_status(status_url)

        return data

    # ── polling ──────────────────────────────────────────────────────────

    def _poll_job_status(self, status_url: str):
        """Poll trạng thái job run cho đến khi đạt trạng thái kết thúc."""
        self.log.info("Starting polling job status...")

        while True:
            resp = requests.get(status_url, headers=self._headers, timeout=30)
            resp.raise_for_status()
            data = self._unwrap_data(resp.json())

            status = (data.get("status") or "").upper()
            self.log.info(
                "Current status: %s  (exit_reason=%s, attempt=%s/%s)",
                status,
                data.get("exit_reason"),
                data.get("attempt_number"),
                data.get("max_attempts"),
            )

            if status in self._SUCCESS_STATES:
                self.log.info("Job completed successfully!")
                return
            elif status in self._FAILURE_STATES:
                raise AirflowException(
                    f"Spark Job failed with status: {status}. "
                    f"exit_reason={data.get('exit_reason')}, error={data.get('error_summary')}"
                )
            elif status in self._PENDING_STATES:
                time.sleep(self.polling_period_seconds)
            else:
                self.log.warning("Unknown status '%s', continuing to poll...", status)
                time.sleep(self.polling_period_seconds)

    # ── on_kill ──────────────────────────────────────────────────────────

    def on_kill(self):
        """
        Gọi cancel endpoint khi Airflow task bị kill.
        POST /api/v1/workspaces/{ws}/spark-jobs/{job}/runs/{runId}/cancel
        """
        if not self._cancel_url or not self._headers:
            self.log.warning("Cannot cancel: cancel_url or headers not initialized.")
            return

        self.log.info("Task bị kill - cancelling job run [%s]...", self._run_id)
        try:
            cancel_resp = requests.post(self._cancel_url, headers=self._headers, timeout=30)
            cancel_resp.raise_for_status()
            self._unwrap_data(cancel_resp.json())
            self.log.info("Job run [%s] cancelled successfully.", self._run_id)
        except Exception as e:
            self.log.error("Failed to cancel job run [%s]: %s", self._run_id, e)
