# Quacknettor Operational Runbook

## Overview
This runbook contains operational procedures for the Quacknettor service.

## 1. Starting and Stopping Services

### Local Development / Testing
To start the dependent services (Postgres and MinIO):
```bash
docker-compose up -d
```
To stop the services and remove containers (preserving volumes):
```bash
docker-compose down
```

### Streamlit Application
Start the Streamlit UI locally:
```bash
streamlit run ui/main.py
```

## 2. Backup Strategy

### Postgres Database
To create a backup of the Postgres database:
```bash
docker exec -t quacknettor-postgres pg_dump -U testuser testdb > db_backup_$(date +%F).sql
```

### MinIO / S3 Data
To backup MinIO buckets, use the `mc` (MinIO Client) tool:
```bash
mc cp -r myminio/testbucket ./backup/testbucket_$(date +%F)
```

## 3. Restore Strategy

### Postgres Database
To restore the Postgres database from a backup:
```bash
cat db_backup_YYYY-MM-DD.sql | docker exec -i quacknettor-postgres psql -U testuser -d testdb
```

### MinIO / S3 Data
To restore MinIO buckets:
```bash
mc cp -r ./backup/testbucket_YYYY-MM-DD/ myminio/testbucket
```

## 4. Troubleshooting
* **Postgres Connection Issues:** Verify connection parameters in `.env` and ensure `docker-compose ps` shows Postgres as 'healthy'.
* **MinIO Issues:** Check MinIO logs using `docker logs quacknettor-minio`.
