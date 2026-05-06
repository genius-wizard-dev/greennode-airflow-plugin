"""GreenNode Operator — trigger và theo dõi Spark Job trên VNG Data Platform."""

import json
import time
from enum import Enum
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from greennode_airflow_plugin.hook import VNGCloudHook

XCOM_WORKSPACE_ID_KEY = "workspace_id"
XCOM_JOB_ID_KEY = "job_id"
XCOM_RUN_ID_KEY = "run_id"


class SparkJobState(str, Enum):
    """Trạng thái Spark Job theo Data Platform API schema."""

    QUEUING = "QUEUING"
    SCHEDULING = "SCHEDULING"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_str(cls, value: str | None) -> "SparkJobState":
        if not value:
            return cls.UNKNOWN
        try:
            return cls(value.upper())
        except ValueError:
            return cls.UNKNOWN

    @property
    def is_final(self) -> bool:
        return self in {self.SUCCESS, self.FAILED, self.CANCELLED}

    @property
    def is_successful(self) -> bool:
        return self == self.SUCCESS


class GreenNodeOperator(BaseOperator):
    """
    Trigger Spark Job trên VNG Data Platform và poll trạng thái cho đến khi hoàn thành.

    API flow:
      1. POST /api/v1/workspaces/{ws}/spark-jobs/{job}/runs
      2. GET  /api/v1/workspaces/{ws}/spark-jobs/{job}/runs/{runId}
      3. POST /api/v1/workspaces/{ws}/spark-jobs/{job}/runs/{runId}/cancel  (on_kill)
    """

    template_fields = ("workspace_id", "job_id", "application_args")
    template_ext = (".json",)

    # GreenNode brand colors (graph view)
    ui_color = "#00B14F"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        workspace_id: str,
        job_id: str,
        application_args: list[str] | dict[str, Any] | str | None = None,
        vng_conn_id: str | None = None,
        token_url: str | None = None,
        data_platform_url: str | None = None,
        polling_period_seconds: int = 15,
        do_xcom_push: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if not workspace_id:
            raise AirflowException("Tham số `workspace_id` là bắt buộc.")
        if not job_id:
            raise AirflowException("Tham số `job_id` là bắt buộc.")

        self.workspace_id = workspace_id
        self.job_id = job_id
        self.application_args = application_args
        self.vng_conn_id = vng_conn_id
        self.token_url = token_url
        self.data_platform_url = data_platform_url
        self.polling_period_seconds = polling_period_seconds
        self.do_xcom_push = do_xcom_push

        self._run_id: str | None = None
        self._hook: VNGCloudHook | None = None

    def _get_hook(self) -> VNGCloudHook:
        if self._hook is None:
            self._hook = VNGCloudHook(
                vng_conn_id=self.vng_conn_id,
                token_url=self.token_url,
                data_platform_url=self.data_platform_url,
            )
        return self._hook

    @staticmethod
    def _normalize_args(value: Any) -> list[str]:
        if value is None:
            return [""]
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            return [json.dumps(value)]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
                if isinstance(parsed, dict):
                    return [json.dumps(parsed)]
            except json.JSONDecodeError:
                pass
            return [value]
        raise AirflowException(f"application_args type không hỗ trợ: {type(value)}")

    def execute(self, context):
        hook = self._get_hook()
        payload = {"application_args": self._normalize_args(self.application_args)}

        self.log.info("Triggering Spark Job [%s] in workspace [%s]", self.job_id, self.workspace_id)
        data = hook.submit_job_run(self.workspace_id, self.job_id, payload)

        self._run_id = data.get("id") or data.get("run_id")
        if not self._run_id:
            raise AirflowException(f"Không tìm thấy run id trong response: {data}")
        self.log.info("Job triggered. run_id=%s", self._run_id)

        if self.do_xcom_push:
            ti = context["ti"]
            ti.xcom_push(key=XCOM_WORKSPACE_ID_KEY, value=self.workspace_id)
            ti.xcom_push(key=XCOM_JOB_ID_KEY, value=self.job_id)
            ti.xcom_push(key=XCOM_RUN_ID_KEY, value=self._run_id)

        self._poll_until_final(hook)
        return data

    def _poll_until_final(self, hook: VNGCloudHook) -> None:
        self.log.info("Bắt đầu polling status (interval=%ss)...", self.polling_period_seconds)
        while True:
            data = hook.get_job_run(self.workspace_id, self.job_id, self._run_id)
            state = SparkJobState.from_str(data.get("status"))
            self.log.info(
                "Status=%s exit_reason=%s attempt=%s/%s",
                state.value,
                data.get("exit_reason"),
                data.get("attempt_number"),
                data.get("max_attempts"),
            )

            if state.is_final:
                if state.is_successful:
                    self.log.info("Job %s hoàn thành thành công.", self._run_id)
                    return
                raise AirflowException(
                    f"Spark Job thất bại với state={state.value} "
                    f"exit_reason={data.get('exit_reason')} error={data.get('error_summary')}"
                )

            time.sleep(self.polling_period_seconds)

    def on_kill(self):
        if not self._run_id:
            self.log.warning("Không có run_id để cancel.")
            return
        self.log.info("Task bị kill — cancelling job run [%s]...", self._run_id)
        try:
            self._get_hook().cancel_job_run(self.workspace_id, self.job_id, self._run_id)
            self.log.info("Cancelled run [%s].", self._run_id)
        except Exception as e:
            self.log.error("Cancel run [%s] thất bại: %s", self._run_id, e)
