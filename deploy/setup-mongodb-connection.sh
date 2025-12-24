#!/usr/bin/env bash
set -e

NAMESPACE=trendgetter
DEPLOYMENT=airflow-scheduler
CONN_ID=mongodb_default

# .env 파일 로드
if [ ! -f .env ]; then
    echo "오류: .env 파일을 찾을 수 없습니다."
    exit 1
fi

set -a
source .env
set +a

# 필수 환경 변수 확인
if [ -z "$MONGODB_USERNAME" ] || [ -z "$MONGODB_PASSWORD" ] || [ -z "$MONGODB_DATABASE" ]; then
    echo "오류: .env 파일에 MONGODB_USERNAME, MONGODB_PASSWORD, MONGODB_DATABASE가 정의되어 있어야 합니다."
    exit 1
fi

# Airflow Scheduler Pod 찾기
echo "Airflow Scheduler Pod 찾는 중..."
POD=$(kubectl get pods -n "$NAMESPACE" \
  -l app=$DEPLOYMENT \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -z "$POD" ]; then
    echo "오류: Airflow Scheduler Pod를 찾을 수 없습니다."
    echo "Pod가 실행 중인지 확인하세요: kubectl get pods -n $NAMESPACE"
    exit 1
fi

echo "Pod 발견: $POD"

# Pod가 Ready 상태가 될 때까지 대기
echo "Pod가 Ready 상태가 될 때까지 대기 중..."
kubectl wait --for=condition=ready pod -n "$NAMESPACE" "$POD" --timeout=300s || {
    echo "경고: Pod가 Ready 상태가 되지 않았습니다."
}

# Python 스크립트 실행 (src 디렉토리가 마운트되어 있으므로 직접 실행 가능)
echo "MongoDB Connection 설정 중..."
kubectl exec -n "$NAMESPACE" "$POD" -c scheduler -- \
  sh -c "cd /opt/airflow && \
    CONN_ID='$CONN_ID' \
    MONGODB_HOST='mongodb' \
    MONGODB_PORT='27017' \
    MONGODB_USERNAME='$MONGODB_USERNAME' \
    MONGODB_PASSWORD='$MONGODB_PASSWORD' \
    MONGODB_DATABASE='$MONGODB_DATABASE' \
    uv run python src/setup_mongodb_connection.py" || {
    echo "오류: Connection 설정에 실패했습니다."
    exit 1
}

# Connection 확인
echo ""
echo "Connection 확인 중..."
kubectl exec -n "$NAMESPACE" "$POD" -c scheduler -- \
  sh -c "uv run python -m airflow connections get $CONN_ID" || {
    echo "경고: Connection을 확인할 수 없습니다."
    exit 1
}

echo ""
echo "✅ MongoDB Connection '$CONN_ID' 설정 완료!"

