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

## 📦 Cài đặt

### Yêu cầu

- Python >= 3.10
- Apache Airflow >= 2.8.0
- `uv` (khuyến nghị) hoặc `pip`

### Cài đặt bằng uv (Khuyến nghị)

```bash
uv pip install greennode-airflow-plugin
```

