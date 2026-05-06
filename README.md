# GreenNode Airflow Plugin

Plugin chính thức cho Apache Airflow để tích hợp với **GreenNode (VNG Cloud)**.

Plugin cung cấp `GreenNodeOperator` giúp bạn dễ dàng trigger và quản lý job trên nền tảng GreenNode một cách chuyên nghiệp, tương tự như các plugin phổ biến khác (iOmete, Astronomer...).

---

## ✨ Tính năng chính

- Tự động xử lý **VNG Cloud IAM Access Token** bên trong Operator
- Hỗ trợ **polling** để chờ job hoàn thành
- Hỗ trợ `do_xcom_push` để truyền dữ liệu giữa các task
- Dễ dàng cấu hình qua **Airflow Connection** hoặc Environment Variable
- Hỗ trợ Jinja templating (`{{ params.xxx }}`)
- Thiết kế theo chuẩn Airflow Plugin (giống iOmete)

---

## 📦 Yêu cầu

- Python >= 3.10
- Apache Airflow >= 2.8.0
- `pip` hoặc `uv`

---

## 🚀 Cách dựng (Setup)

Plugin được register thông qua entry-point group `airflow.plugins` trong `pyproject.toml`:

```toml
[project.entry-points."airflow.plugins"]
greennode_airflow_plugin = "greennode_airflow_plugin.plugin:GreenNodePlugin"
```

Airflow sẽ tự discover plugin sau khi package được `pip install`. Có 3 cách dựng tuỳ môi trường:

### 1. Local / Standalone Airflow

```bash
pip install git+https://github.com/genius-wizard-dev/greennode-airflow-plugin.git@main
# hoặc cài từ source
pip install -e .
```

Sau khi cài, restart Airflow và verify:

```bash
airflow plugins
# Phải thấy: greennode_airflow_plugin
```

### 2. Airflow trên Kubernetes — Dev (nhanh, không cần registry)

Dùng env `_PIP_ADDITIONAL_REQUIREMENTS` của image `apache/airflow` để pip install từ git mỗi lần pod khởi động.

Trong `values.yaml` / `override.yaml` của Helm chart:

```yaml
env:
  - name: _PIP_ADDITIONAL_REQUIREMENTS
    value: "git+https://github.com/genius-wizard-dev/greennode-airflow-plugin.git@main"
```

Apply:

```bash
helm upgrade <release> . -n <namespace> -f override.yaml
kubectl -n <namespace> rollout restart deploy sts
```

Verify:

```bash
kubectl -n <namespace> exec deploy/<release>-scheduler -- airflow plugins
```

**Lưu ý**:
- Pod khởi động chậm hơn ~10–30s do phải pip install lại mỗi lần.
- Repo private cần token: `git+https://<token>@github.com/...`.
- Chỉ phù hợp dev. Production nên dùng cách 3.

### 3. Airflow trên Kubernetes — Production (build custom image)

Tạo `Dockerfile`:

```dockerfile
FROM apache/airflow:3.2.0
RUN pip install --no-cache-dir \
    git+https://github.com/genius-wizard-dev/greennode-airflow-plugin.git@<tag>
```

Build & push lên registry:

```bash
docker build -t <registry>/airflow-greennode:<tag> .
docker push <registry>/airflow-greennode:<tag>
```

Trong `override.yaml`:

```yaml
defaultAirflowRepository: <registry>/airflow-greennode
defaultAirflowTag: "<tag>"

images:
  airflow:
    repository: <registry>/airflow-greennode
    tag: "<tag>"
    pullPolicy: IfNotPresent
  pod_template:
    repository: <registry>/airflow-greennode
    tag: "<tag>"
    pullPolicy: IfNotPresent
```

Apply:

```bash
helm upgrade <release> . -n <namespace> -f override.yaml
kubectl -n <namespace> rollout restart deploy sts
```

> **Quan trọng**: Khi dùng `KubernetesExecutor`, image của `pod_template` cũng phải có plugin (vì task pod sinh từ template này). Đã được set đồng bộ trong block trên.

---

## 🔌 Cấu hình Connection

Trong Airflow UI: **Admin → Connections → Add**:

- **Conn Id**: `greennode_default`
- **Conn Type**: `HTTP`
- **Host**: `https://api.greennode.ai`
- **Login**: `<VNG_CLOUD_CLIENT_ID>`
- **Password**: `<VNG_CLOUD_CLIENT_SECRET>`

Hoặc dùng env:

```bash
export AIRFLOW_CONN_GREENNODE_DEFAULT='http://<client_id>:<client_secret>@api.greennode.ai'
```

---

## 🧑‍💻 Sử dụng trong DAG

```python
from airflow import DAG
from datetime import datetime
from greennode_airflow_plugin.greennode_operator import GreenNodeOperator

with DAG(
    dag_id="example_greennode",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_job = GreenNodeOperator(
        task_id="run_greennode_job",
        conn_id="greennode_default",
        job_id="my-job-id",
        params={"input": "value"},
        wait_for_completion=True,
        poll_interval=10,
        do_xcom_push=True,
    )
```

---

## 🛠️ Phát triển

```bash
git clone https://github.com/genius-wizard-dev/greennode-airflow-plugin.git
cd greennode-airflow-plugin
uv pip install -e .
```

Cấu trúc:

```
greennode_airflow_plugin/
├── __init__.py
├── plugin.py              # AirflowPlugin class
├── greennode_operator.py  # GreenNodeOperator
└── hook.py                # GreenNodeHook (IAM token, HTTP client)
```

Sau khi sửa code, push lên `main`:

```bash
git add .
git commit -m "feat: ..."
git push origin main
```

Trên cluster K8s (dev cách 2): chỉ cần `kubectl rollout restart` — pod sẽ pip install lại từ git commit mới nhất.

---

## 🐛 Troubleshooting

**`airflow plugins` không hiển thị plugin**

1. Kiểm tra entry-point group phải là `airflow.plugins` (KHÔNG phải `apache_airflow_plugin`):
   ```toml
   [project.entry-points."airflow.plugins"]
   ```
2. Kiểm tra package đã cài thật chưa:
   ```bash
   pip show greennode-airflow-plugin
   ```
3. Xem log entrypoint xem pip install có lỗi không:
   ```bash
   kubectl -n <ns> logs deploy/<release>-scheduler | grep -iE "pip|greennode"
   ```

**Task pod (KubernetesExecutor) không có plugin**

- Đảm bảo `images.pod_template` cùng image với scheduler/worker.
- Hoặc env `_PIP_ADDITIONAL_REQUIREMENTS` được set ở top-level `env:` để propagate xuống task pod.

**Repo private, pip clone fail**

```yaml
env:
  - name: _PIP_ADDITIONAL_REQUIREMENTS
    value: "git+https://<github_pat>@github.com/genius-wizard-dev/greennode-airflow-plugin.git@main"
```

Nên đặt token qua Kubernetes Secret thay vì hardcode trong values.

---

## 📄 License

MIT
