#!/bin/bash

# Namespace 생성 (없으면 생성)
kubectl apply -f deploy/namespace.yaml

# Superset을 제외한 모든 Pod 삭제 (재시작을 위해)
echo "Superset을 제외한 Pod 삭제 중..."
kubectl delete pod -n trendgetter -l app=postgres 2>/dev/null || true
kubectl delete pod -n trendgetter -l app=mongodb 2>/dev/null || true
kubectl delete pod -n trendgetter -l app=redis 2>/dev/null || true
kubectl delete pod -n trendgetter -l app=airflow-scheduler 2>/dev/null || true
kubectl delete pod -n trendgetter -l app=airflow-webserver 2>/dev/null || true
kubectl delete pod -n trendgetter -l app=airflow-worker 2>/dev/null || true

# PV/PVC 정리 (Released 상태 방지)
kubectl delete pvc --all -n trendgetter 2>/dev/null || true
kubectl delete pv --all 2>/dev/null || true

# .env 파일에서 ConfigMap과 Secret 생성
kubectl create configmap app-env --from-env-file=.env -n trendgetter --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic airflow-secret --from-env-file=.env -n trendgetter --dry-run=client -o yaml | kubectl apply -f -

# .env 파일 로드 (환경 변수 치환이 필요한 리소스 적용 전에)
set -a
source .env
set +a

# PV/PVC 적용 (PV 먼저, 그 다음 PVC)
kubectl apply -f deploy/pv.dev.yaml
sleep 1
kubectl apply -f deploy/pvc.yaml
kubectl apply -f deploy/redis.yaml
# postgres.yaml은 환경 변수 치환이 필요하므로 envsubst 사용
envsubst < deploy/postgres.yaml | kubectl apply -f -
# mongodb.yaml은 환경 변수 치환이 필요하므로 envsubst 사용
envsubst < deploy/mongodb.yaml | kubectl apply -f -

# Postgres가 준비될 때까지 대기
echo "Postgres가 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=postgres -n trendgetter --timeout=300s

# MongoDB가 준비될 때까지 대기
echo "MongoDB가 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=mongodb -n trendgetter --timeout=300s

# Airflow 리소스 적용 (initContainer가 자동으로 DB 초기화)
kubectl apply -f deploy/airflow-scheduler.yaml
# airflow-webserver.yaml은 환경 변수 치환이 필요하므로 envsubst 사용
envsubst < deploy/airflow-webserver.yaml | kubectl apply -f -
kubectl apply -f deploy/airflow-worker.yaml

# Airflow Webserver가 준비될 때까지 대기
echo "Airflow Webserver가 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=airflow-webserver -n trendgetter --timeout=600s || echo "경고: Webserver가 아직 준비되지 않았습니다."

# Superset 데이터베이스 생성
echo "Superset 데이터베이스 생성 중..."
bash deploy/setup-superset-db.sh || echo "경고: Superset 데이터베이스 생성 중 오류 발생 (무시됨)"

# Superset 리소스 적용
echo "Superset 배포 중..."
envsubst < deploy/superset.yaml | kubectl apply -f -

# Superset이 준비될 때까지 대기
echo "Superset이 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=superset -n trendgetter --timeout=600s || echo "경고: Superset이 아직 준비되지 않았습니다."


