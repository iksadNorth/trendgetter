#!/usr/bin/env bash
set -e

NAMESPACE=trendgetter
DEPLOYMENT=airflow-scheduler

echo "PostgreSQL 스키마 마이그레이션 실행 중..."

# Airflow scheduler pod 찾기
POD=$(kubectl get pods -n "$NAMESPACE" \
  -l app=$DEPLOYMENT \
  -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD" ]; then
    echo "오류: Airflow scheduler Pod를 찾을 수 없습니다."
    exit 1
fi

echo "Pod: $POD"

# .env 파일에서 환경 변수 로드 (선택적)
if [ -f .env ]; then
    echo ".env 파일에서 환경 변수 로드 중..."
    set -a
    source .env
    set +a
else
    echo "경고: .env 파일을 찾을 수 없습니다. Pod의 환경 변수를 사용합니다."
fi

# superset 데이터베이스 존재 확인 (선택적)
echo "superset 데이터베이스 확인 중..."
POSTGRES_POD=$(kubectl get pods -n "$NAMESPACE" -l app=postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$POSTGRES_POD" ]; then
    POSTGRES_USER=${POSTGRES_USER:-airflow}
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U "$POSTGRES_USER" -d postgres -c "SELECT 1 FROM pg_database WHERE datname='superset';" > /dev/null 2>&1 || {
        echo "경고: superset 데이터베이스가 존재하지 않습니다."
        echo "      deploy/setup-superset-db.sh를 먼저 실행하세요."
        exit 1
    }
    echo "✅ superset 데이터베이스 확인 완료"
fi

# Alembic 마이그레이션 실행
echo ""
echo "Alembic 마이그레이션 실행 중..."
echo "작업 디렉토리: /opt/airflow/src"
echo ""

kubectl exec -n "$NAMESPACE" "$POD" -- \
  sh -c "
    cd /opt/airflow/src && \
    uv run alembic -c alembic.ini upgrade head
  "

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ PostgreSQL 스키마 마이그레이션 완료!"
else
    echo ""
    echo "❌ 마이그레이션 실행 중 오류 발생"
    exit 1
fi

