#!/bin/bash
# PostgreSQL 데이터 완전 초기화 스크립트

set -e

NAMESPACE=trendgetter
POSTGRES_DATA_PATH=/home/iksadnorth/project/trendgetter/postgres-data

echo "⚠️  PostgreSQL 데이터를 완전히 초기화합니다."
echo "이 작업은 되돌릴 수 없습니다!"
read -p "계속하시겠습니까? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "취소되었습니다."
    exit 0
fi

echo ""
echo "1. PostgreSQL StatefulSet 삭제 중..."
kubectl delete statefulset postgres -n "$NAMESPACE" --ignore-not-found=true || true

echo "2. PostgreSQL Pod 삭제 중..."
kubectl delete pod -l app=postgres -n "$NAMESPACE" --ignore-not-found=true || true

echo "3. Pod가 완전히 종료될 때까지 대기 중..."
sleep 5

echo "4. PVC 삭제 중..."
kubectl delete pvc postgres-data -n "$NAMESPACE" --ignore-not-found=true || true

echo "5. PV 삭제 중..."
kubectl delete pv postgres-data-pv --ignore-not-found=true || true

echo "6. 호스트 디렉토리 데이터 삭제 중..."
if [ -d "$POSTGRES_DATA_PATH" ]; then
    sudo rm -rf "$POSTGRES_DATA_PATH"
    echo "   ✅ $POSTGRES_DATA_PATH 삭제 완료"
else
    echo "   ℹ️  $POSTGRES_DATA_PATH 디렉토리가 존재하지 않습니다."
fi

echo ""
echo "7. 디렉토리 재생성 중..."
sudo mkdir -p "$POSTGRES_DATA_PATH"
sudo chmod 777 "$POSTGRES_DATA_PATH"

echo ""
echo "✅ PostgreSQL 데이터 초기화 완료!"
echo ""
echo "다음 단계:"
echo "  ./deploy/apply.sh 를 실행하여 PostgreSQL을 다시 배포하세요."

