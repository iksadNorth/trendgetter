#!/usr/bin/env bash
set -e

NAMESPACE=trendgetter
DEPLOYMENT=airflow-scheduler
CONN_ID=superset_postgres_default

# .env 파일 로드
if [ ! -f .env ]; then
    echo "오류: .env 파일을 찾을 수 없습니다."
    exit 1
fi

set -a
source .env
set +a

# 필수 환경 변수 확인
if [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_PASSWORD" ]; then
    echo "오류: .env 파일에 POSTGRES_USER, POSTGRES_PASSWORD가 정의되어 있어야 합니다."
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

# PostgreSQL Connection 설정 (postgres 타입으로 생성, superset 데이터베이스 사용)
echo "PostgreSQL Connection 설정 중..."
POSTGRES_HOST=${POSTGRES_HOST:-postgres.trendgetter.svc.cluster.local}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
SUPERSET_DATABASE=${SUPERSET_DATABASE:-superset}

kubectl exec -n "$NAMESPACE" "$POD" -c scheduler -- \
  sh -c "cd /opt/airflow && \
    uv run airflow connections delete $CONN_ID 2>/dev/null || true && \
    uv run airflow connections add $CONN_ID \
      --conn-type postgres \
      --conn-host '$POSTGRES_HOST' \
      --conn-port $POSTGRES_PORT \
      --conn-login '$POSTGRES_USER' \
      --conn-password '$POSTGRES_PASSWORD' \
      --conn-schema '$SUPERSET_DATABASE'" || {
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
echo "✅ PostgreSQL Connection '$CONN_ID' 설정 완료!"
echo "   Host: $POSTGRES_HOST"
echo "   Port: $POSTGRES_PORT"
echo "   Database: $POSTGRES_DB"
echo "   User: $POSTGRES_USER"

