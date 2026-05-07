# GreenNode Airflow Plugin

Apache Airflow plugin để trigger và quản lý **Spark Job trên VNG Cloud Data Platform (GreenNode)**.

Plugin cung cấp `GreenNodeOperator` xử lý toàn bộ vòng đời job:

1. Lấy IAM Access Token (OAuth2 client credentials, auto-refresh)
2. Submit Spark Job run
3. Poll status đến khi job kết thúc
4. Cancel job nếu Airflow task bị kill

---

## Installation

**Từ PyPI (production):**

```bash
pip install greennode-airflow-plugin
```

**Từ TestPyPI (đang dev / chưa release lên PyPI):**

```bash
pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  greennode-airflow-plugin==0.3.3
```

> `--extra-index-url` là **bắt buộc** — TestPyPI không có `apache-airflow` / `requests`, phải fallback sang PyPI thật để cài dependencies.

**Từ source (local dev):**

```bash
git clone https://github.com/genius-wizard-dev/greennode-airflow-plugin.git
cd greennode-airflow-plugin
pip install -e ".[dev]"
```

Verify đã cài thành công:

```bash
pip show greennode-airflow-plugin
python -c "from greennode_airflow_plugin import GreenNodeOperator, VNGCloudHook; print('OK')"
```

Hoặc check qua Airflow CLI:

```bash
airflow plugins
# Phải thấy dòng:
# greennode | greennode-airflow-plugin==0.3.3: EntryPoint(name='greennode', value='greennode_airflow_plugin.plugin:GreenNodePlugin', group='airflow.plugins')
```

### Cài trên Airflow Helm chart (dev)

Trong `values.yaml` / `override.yaml`:

```yaml
env:
  - name: _PIP_ADDITIONAL_REQUIREMENTS
    value: "--index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ greennode-airflow-plugin==0.3.3"
```

```bash
helm upgrade <release> . -n <namespace> -f override.yaml
```

Helm sẽ rolling restart pods (scheduler/worker/triggerer/api-server) → mỗi pod khi start sẽ `pip install` plugin từ TestPyPI.

> Khi đã release lên PyPI thật, đơn giản hoá thành: `value: "greennode-airflow-plugin==0.3.3"`.

### Cài trên Airflow Helm chart (production)

Build custom image — xem [Kubernetes (Production)](#kubernetes-production) ở dưới.

---

## Configuration

Tạo Airflow Connection (UI: **Admin → Connections → Add**):

| Field           | Giá trị                                                        |
| --------------- | -------------------------------------------------------------- |
| Connection Id   | `vng_cloud_default` _(default — có thể đổi qua `vng_conn_id`)_ |
| Connection Type | `Generic`                                                      |
| Host            | `https://dev-iam-proxy.dataplatform.vngcloud.tech`                    |
| Login           | `<VNG_CLIENT_ID>`                                              |
| Password        | `<VNG_CLIENT_SECRET>`                                          |
| Extra (JSON)    | xem dưới                                                       |

**Extra** (JSON, optional — override default URL):

```json
{
  "data_platform_url": "https://dev-backend-proxy.dataplatform.vngcloud.tech",
  "token_path": "/accounts-api/v2/auth/token"
}
```

### Tạo Connection bằng env (CLI / Helm)

```bash
export AIRFLOW_CONN_VNG_CLOUD_DEFAULT='{
  "conn_type": "generic",
  "host": "https://dev-iam-proxy.dataplatform.vngcloud.tech",
  "login": "<CLIENT_ID>",
  "password": "<CLIENT_SECRET>",
  "extra": {
    "data_platform_url": "https://dev-backend-proxy.dataplatform.vngcloud.tech"
  }
}'
```

Trong Helm chart, đặt qua Kubernetes Secret:

```bash
kubectl -n airflow create secret generic vng-cloud-conn \
  --from-literal=AIRFLOW_CONN_VNG_CLOUD_DEFAULT='{"conn_type":"generic","host":"https://dev-iam-proxy.dataplatform.vngcloud.tech","login":"<CID>","password":"<CSECRET>","extra":{"data_platform_url":"https://dev-backend-proxy.dataplatform.vngcloud.tech"}}'
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

Xem thêm [`dags/example_greennode.py`](./dags/example_greennode.py) (full) hoặc [`dags/example_greennode_minimal.py`](./dags/example_greennode_minimal.py) (minimal).

### `GreenNodeOperator` parameters

| Parameter                | Required | Default                 | Description                                       |
| ------------------------ | -------- | ----------------------- | ------------------------------------------------- |
| `workspace_id`           | ✅       | —                       | Workspace ID (templated)                          |
| `job_id`                 | ✅       | —                       | Spark Job ID (templated)                          |
| `application_args`       | ❌       | `[""]`                  | `list[str]`, `dict`, hoặc JSON string (templated) |
| `vng_conn_id`            | ❌       | `vng_cloud_default`     | Airflow Connection ID                             |
| `token_url`              | ❌       | từ Connection / default | Override IAM token endpoint                       |
| `data_platform_url`      | ❌       | từ Connection / default | Override Data Platform base URL                   |
| `polling_period_seconds` | ❌       | `15`                    | Thời gian giữa các lần poll status                |
| `do_xcom_push`           | ❌       | `False`                 | Push `workspace_id`, `job_id`, `run_id` qua XCom  |

**Templated fields**: `workspace_id`, `job_id`, `application_args` — hỗ trợ Jinja `{{ ds }}`, `{{ params.x }}`, `{{ var.value.* }}`.

**Template extension**: `.json` — file `.json` sẽ được auto-load.

### XCom keys

| Key            | Constant                | Mô tả        |
| -------------- | ----------------------- | ------------ |
| `workspace_id` | `XCOM_WORKSPACE_ID_KEY` | Workspace ID |
| `job_id`       | `XCOM_JOB_ID_KEY`       | Spark Job ID |
| `run_id`       | `XCOM_RUN_ID_KEY`       | Run ID       |

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

Khi release ổn định, **không nên** dùng `_PIP_ADDITIONAL_REQUIREMENTS` (chậm pod start, phụ thuộc network mỗi lần khởi động). Build custom image:

```dockerfile
FROM apache/airflow:3.2.0
RUN pip install --no-cache-dir greennode-airflow-plugin==0.3.3
```

```bash
docker build -t <registry>/airflow-greennode:0.3.3 .
docker push <registry>/airflow-greennode:0.3.3
```

```yaml
# override.yaml
defaultAirflowRepository: <registry>/airflow-greennode
defaultAirflowTag: "0.3.3"

images:
  airflow:
    repository: <registry>/airflow-greennode
    tag: "0.3.3"
    pullPolicy: IfNotPresent
  pod_template:
    repository: <registry>/airflow-greennode
    tag: "0.3.3"
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
│   ├── __init__.py                   # Package metadata, public exports
│   ├── plugin.py                     # AirflowPlugin registration (hooks + operators)
│   ├── greennode_operator.py         # GreenNodeOperator + SparkJobState
│   └── hook.py                       # VNGCloudHook (IAM, Data Platform API)
├── dags/
│   ├── example_greennode.py          # Full example (templating, Variables)
│   └── example_greennode_minimal.py  # Minimal example
├── tests/
│   ├── test_state.py
│   └── test_operator.py
├── .github/workflows/
│   └── publish.yml                   # Auto-publish PyPI/TestPyPI via OIDC
├── Makefile
├── pyproject.toml
└── README.md
```

### Release workflow

```bash
# 1. Bump version trong pyproject.toml (ví dụ 0.3.3 → 0.3.4)
# 2. Commit + tag + push
git commit -am "Release v0.3.4"
git tag v0.3.4
git push origin main && git push origin v0.3.4
```

GitHub Actions sẽ tự build và publish lên TestPyPI. Để publish lên PyPI thật: vào tab **Actions → Publish to PyPI → Run workflow → target = pypi**.

---

## Troubleshooting

**`ModuleNotFoundError: greennode_airflow_plugin`**

1. Verify package đã cài:
   ```bash
   pip show greennode-airflow-plugin
   ```
2. Trên Kubernetes, check pod có install được không (lúc start mới chạy `pip install`):
   ```bash
   kubectl -n airflow logs deploy/airflow-scheduler -c scheduler | grep -iE "pip|greennode"
   ```
3. Nếu dùng `_PIP_ADDITIONAL_REQUIREMENTS` mà version trong `pyproject.toml` không bump, pip có thể cache → restart pod không reinstall. Bump version (vd `0.3.3` → `0.3.4`) hoặc đổi sang Docker image-based (xem [Kubernetes (Production)](#kubernetes-production)).

**Task pod (KubernetesExecutor) không có plugin**

- Đảm bảo `images.pod_template` cùng image với scheduler/worker.
- Hoặc set env `_PIP_ADDITIONAL_REQUIREMENTS` ở top-level `env:` để propagate xuống task pod.

**Token request fail (401/403)**

- Kiểm tra `Host` field của Connection có đúng IAM endpoint base không (mặc định: `https://dev-iam-proxy.dataplatform.vngcloud.tech`).
- Kiểm tra `client_id` / `client_secret` trong VNG Cloud IAM console.
- Xác minh cả `client_id` lẫn `client_secret` thuộc cùng environment (dev / prod).

**Lỗi parse DAG: "Don't use runtime-varying value as argument in Dag constructor"**

- Không dùng `pendulum.today()`, `datetime.now()`, `Variable.get(...)` trực tiếp trong `DAG(...)` / `Operator(...)` args.
- Dùng giá trị tĩnh (`pendulum.datetime(2026, 1, 1, tz="UTC")`) hoặc Jinja template (`"{{ ds }}"`, `"{{ var.value.x }}"`).

---

## License

Apache 2.0
