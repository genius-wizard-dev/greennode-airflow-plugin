# GreenNode Airflow Plugin

Apache Airflow plugin để trigger và quản lý **Spark Job trên VNG Cloud Data Platform (GreenNode)**.

Plugin cung cấp `GreenNodeOperator` xử lý toàn bộ vòng đời job:

1. Lấy IAM Access Token (OAuth2 client credentials, auto-refresh)
2. Submit Spark Job run
3. Poll status đến khi job kết thúc
4. Cancel job nếu Airflow task bị kill

---

## Installation

```bash
pip install git+https://github.com/genius-wizard-dev/greennode-airflow-plugin.git@main
```

Hoặc cài từ source:

```bash
git clone https://github.com/genius-wizard-dev/greennode-airflow-plugin.git
cd greennode-airflow-plugin
pip install -e .
```

Restart Airflow scheduler/worker và verify:

```bash
airflow plugins
# Phải thấy plugin "greennode" với operator GreenNodeOperator + hook VNGCloudHook
```

### Cài trên Airflow Helm chart (dev)

Trong `values.yaml` / `override.yaml`:

```yaml
env:
  - name: _PIP_ADDITIONAL_REQUIREMENTS
    value: "git+https://github.com/genius-wizard-dev/greennode-airflow-plugin.git@main"
```

```bash
helm upgrade <release> . -n <namespace> -f override.yaml
kubectl -n <namespace> rollout restart deploy sts
```

### Cài trên Airflow Helm chart (production)

Build custom image — xem [docs/k8s-production.md](#kubernetes-production) ở dưới.

---

## Configuration

Tạo Airflow Connection (UI: **Admin → Connections → Add**):

| Field            | Giá trị                                                            |
| ---------------- | ------------------------------------------------------------------ |
| Connection Id    | `vng_cloud_default` *(default — có thể đổi qua `vng_conn_id`)*     |
| Connection Type  | `Generic`                                                          |
| Host             | `https://pub-iamapis.api-dev.vngcloud.tech`                        |
| Login            | `<VNG_CLIENT_ID>`                                                  |
| Password         | `<VNG_CLIENT_SECRET>`                                              |
| Extra (JSON)     | xem dưới                                                           |

**Extra** (JSON, optional — override default URL):

```json
{
  "data_platform_url": "https://dataplatform.api-dev.vngcloud.tech",
  "token_path": "/accounts-api/v2/auth/token"
}
```

### Tạo Connection bằng env (CLI / Helm)

```bash
export AIRFLOW_CONN_VNG_CLOUD_DEFAULT='{
  "conn_type": "generic",
  "host": "https://pub-iamapis.api-dev.vngcloud.tech",
  "login": "<CLIENT_ID>",
  "password": "<CLIENT_SECRET>",
  "extra": {
    "data_platform_url": "https://dataplatform.api-dev.vngcloud.tech"
  }
}'
```

Trong Helm chart, đặt qua Kubernetes Secret:

```bash
kubectl -n airflow create secret generic vng-cloud-conn \
  --from-literal=AIRFLOW_CONN_VNG_CLOUD_DEFAULT='{"conn_type":"generic","host":"https://pub-iamapis.api-dev.vngcloud.tech","login":"<CID>","password":"<CSECRET>","extra":{"data_platform_url":"https://dataplatform.api-dev.vngcloud.tech"}}'
```

```yaml
# override.yaml
env:
  - name: AIRFLOW_CONN_VNG_CLOUD_DEFAULT
    valueFrom:
      secretKeyRef:
        name: vng-cloud-conn
        key: AIRFLOW_CONN_VNG_CLOUD_DEFAULT
```

### Fallback bằng env (dev nhanh, không khuyến khích production)

Nếu connection không tồn tại, hook sẽ đọc credentials từ env:

```bash
export VNG_CLIENT_ID="..."
export VNG_CLIENT_SECRET="..."
```

---

## Usage

```python
from datetime import datetime
from airflow import DAG
from greennode_airflow_plugin import GreenNodeOperator

with DAG(
    dag_id="example_greennode_spark_job",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["greennode"],
) as dag:

    run_job = GreenNodeOperator(
        task_id="run_spark_job",
        workspace_id="ws-abc-123",
        job_id="job-xyz-456",
        application_args=["--date", "{{ ds }}", "--mode", "prod"],
        polling_period_seconds=15,
        do_xcom_push=True,
    )
```

Xem thêm [`dags/example_greennode.py`](./dags/example_greennode.py).

### `GreenNodeOperator` parameters

| Parameter                | Required | Default              | Description                                                       |
| ------------------------ | -------- | -------------------- | ----------------------------------------------------------------- |
| `workspace_id`           | ✅       | —                    | Workspace ID (templated)                                          |
| `job_id`                 | ✅       | —                    | Spark Job ID (templated)                                          |
| `application_args`       | ❌       | `[""]`               | `list[str]`, `dict`, hoặc JSON string (templated)                 |
| `vng_conn_id`            | ❌       | `vng_cloud_default`  | Airflow Connection ID                                             |
| `token_url`              | ❌       | từ Connection / default | Override IAM token endpoint                                    |
| `data_platform_url`      | ❌       | từ Connection / default | Override Data Platform base URL                                |
| `polling_period_seconds` | ❌       | `15`                 | Thời gian giữa các lần poll status                                |
| `do_xcom_push`           | ❌       | `False`              | Push `workspace_id`, `job_id`, `run_id` qua XCom                  |

**Templated fields**: `workspace_id`, `job_id`, `application_args` — hỗ trợ Jinja `{{ ds }}`, `{{ params.x }}`, `{{ var.value.* }}`.

**Template extension**: `.json` — file `.json` sẽ được auto-load.

### XCom keys

| Key            | Constant                  | Mô tả          |
| -------------- | ------------------------- | -------------- |
| `workspace_id` | `XCOM_WORKSPACE_ID_KEY`   | Workspace ID   |
| `job_id`       | `XCOM_JOB_ID_KEY`         | Spark Job ID   |
| `run_id`       | `XCOM_RUN_ID_KEY`         | Run ID         |

### Spark Job states

```python
from greennode_airflow_plugin import SparkJobState

SparkJobState.SUCCESS.is_final        # True
SparkJobState.SUCCESS.is_successful   # True
SparkJobState.RUNNING.is_final        # False
```

| State        | Final? | Success? |
| ------------ | ------ | -------- |
| `QUEUING`    | ❌     | —        |
| `SCHEDULING` | ❌     | —        |
| `PENDING`    | ❌     | —        |
| `RUNNING`    | ❌     | —        |
| `SUCCESS`    | ✅     | ✅       |
| `FAILED`     | ✅     | ❌       |
| `CANCELLED`  | ✅     | ❌       |

---

## Kubernetes (Production)

Build custom image (recommend):

```dockerfile
FROM apache/airflow:3.2.0
RUN pip install --no-cache-dir \
    git+https://github.com/genius-wizard-dev/greennode-airflow-plugin.git@v0.2.0
```

```bash
docker build -t <registry>/airflow-greennode:0.2.0 .
docker push <registry>/airflow-greennode:0.2.0
```

```yaml
# override.yaml
defaultAirflowRepository: <registry>/airflow-greennode
defaultAirflowTag: "0.2.0"

images:
  airflow:
    repository: <registry>/airflow-greennode
    tag: "0.2.0"
    pullPolicy: IfNotPresent
  pod_template:
    repository: <registry>/airflow-greennode
    tag: "0.2.0"
    pullPolicy: IfNotPresent
```

> **KubernetesExecutor**: image `pod_template` cũng phải chứa plugin (task pod sinh từ template này).

---

## Development

```bash
git clone https://github.com/genius-wizard-dev/greennode-airflow-plugin.git
cd greennode-airflow-plugin
make install          # uv pip install -e ".[dev]"
make test             # pytest
make lint             # ruff + mypy
make format           # black + isort
make build            # build wheel
```

### Project structure

```
greennode-airflow-plugin/
├── greennode_airflow_plugin/
│   ├── __init__.py            # Package metadata, public exports
│   ├── plugin.py              # AirflowPlugin registration
│   ├── greennode_operator.py  # GreenNodeOperator + SparkJobState
│   └── hook.py                # VNGCloudHook (IAM, Data Platform API)
├── dags/
│   └── example_greennode.py
├── tests/
│   ├── test_state.py
│   └── test_operator.py
├── Makefile
├── pyproject.toml
└── README.md
```

---

## Troubleshooting

**`airflow plugins` không thấy plugin**

1. Entry-point group phải là `airflow.plugins`:
   ```toml
   [project.entry-points."airflow.plugins"]
   greennode = "greennode_airflow_plugin.plugin:GreenNodePlugin"
   ```
2. Verify package đã cài:
   ```bash
   pip show greennode-airflow-plugin
   ```
3. Check log entrypoint:
   ```bash
   kubectl -n airflow logs deploy/airflow-scheduler | grep -iE "pip|greennode"
   ```

**Task pod (KubernetesExecutor) không có plugin**

- Đảm bảo `images.pod_template` cùng image với scheduler/worker.
- Hoặc set env `_PIP_ADDITIONAL_REQUIREMENTS` ở top-level `env:` để propagate.

**Repo private, pip clone fail**

```yaml
env:
  - name: _PIP_ADDITIONAL_REQUIREMENTS
    value: "git+https://${GITHUB_TOKEN}@github.com/genius-wizard-dev/greennode-airflow-plugin.git@main"
```

Token nên đặt qua Kubernetes Secret, không hardcode.

**Token request fail (401/403)**

- Kiểm tra `Host` field của Connection có đúng IAM endpoint base không (mặc định: `https://pub-iamapis.api-dev.vngcloud.tech`).
- Kiểm tra `client_id` / `client_secret` trong VNG Cloud IAM console.

---

## License

Apache 2.0
