#!/bin/bash
set -e

NAMESPACE=trendgetter
POSTGRES_POD=$(kubectl get pods -n "$NAMESPACE" -l app=postgres -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POSTGRES_POD" ]; then
    echo "오류: PostgreSQL Pod를 찾을 수 없습니다."
    exit 1
fi

echo "PostgreSQL에 superset 데이터베이스 생성 중..."

# .env 파일에서 PostgreSQL 자격증명 로드
if [ -f .env ]; then
    set -a
    source .env
    set +a
else
    echo "경고: .env 파일을 찾을 수 없습니다. 기본값을 사용합니다."
    POSTGRES_USER=${POSTGRES_USER:-airflow}
    POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
fi

# superset 데이터베이스 생성 (이미 존재하면 무시)
kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE superset;" 2>/dev/null || echo "superset 데이터베이스가 이미 존재하거나 생성 중 오류 발생 (무시됨)"

echo "superset 데이터베이스 생성 완료."


