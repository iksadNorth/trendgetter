#!/bin/bash

# Namespace 적용
kubectl delete namespace trendgetter || true

# PV/PVC 정리 (Released 상태 방지)
kubectl delete pvc --all -n trendgetter 2>/dev/null || true
kubectl delete pv --all 2>/dev/null || true

kubectl apply -f deploy/namespace.yaml

# .env 파일에서 ConfigMap과 Secret 생성
kubectl create configmap app-env --from-env-file=.env -n trendgetter --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic airflow-secret --from-env-file=.env -n trendgetter --dry-run=client -o yaml | kubectl apply -f -

# PV/PVC 적용 (PV 먼저, 그 다음 PVC)
kubectl apply -f deploy/pv.dev.yaml
sleep 1
kubectl apply -f deploy/pvc.yaml
kubectl apply -f deploy/redis.yaml
kubectl apply -f deploy/postgres.yaml

# Postgres가 준비될 때까지 대기
echo "Postgres가 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=postgres -n trendgetter --timeout=300s

# .env 파일 로드
set -a
source .env
set +a

# Airflow 리소스 적용 (initContainer가 자동으로 DB 초기화)
kubectl apply -f deploy/airflow-scheduler.yaml
# airflow-webserver.yaml은 환경 변수 치환이 필요하므로 envsubst 사용
envsubst < deploy/airflow-webserver.yaml | kubectl apply -f -
kubectl apply -f deploy/airflow-worker.yaml

# Airflow Webserver가 준비될 때까지 대기
echo "Airflow Webserver가 준비될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -l app=airflow-webserver -n trendgetter --timeout=600s || echo "경고: Webserver가 아직 준비되지 않았습니다."

# Airflow Webserver 접근 정보 출력
echo ""
echo "=========================================="
echo "Airflow Webserver 접근 방법"
echo "=========================================="
echo "다음 명령어를 실행하세요:"
echo "  kubectl port-forward -n trendgetter svc/airflow-webserver 8080:8080"
echo "그 다음 브라우저에서 http://localhost:8080 접속"
echo ""
echo "또는 minikube tunnel을 사용 중이라면:"
echo "  curl http://127.0.0.1:8080"
echo "=========================================="

