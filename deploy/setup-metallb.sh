#!/bin/bash

# MetalLB 설치 스크립트

echo "MetalLB 설치 중..."

# MetalLB 네임스페이스 생성
kubectl create namespace metallb-system --dry-run=client -o yaml | kubectl apply -f -

# MetalLB 설치 (v0.13.12 버전)
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.12/config/manifests/metallb-native.yaml

# MetalLB 컨트롤러가 준비될 때까지 대기
echo "MetalLB 컨트롤러가 준비될 때까지 대기 중..."
kubectl wait --namespace metallb-system \
  --for=condition=ready pod \
  --selector=app=metallb \
  --timeout=90s

# IPPool 설정 적용
echo "MetalLB IPPool 설정 적용 중..."
kubectl apply -f deploy/metallb-config.yaml

echo ""
echo "MetalLB 설치 완료!"
echo "다음 명령어로 상태를 확인하세요:"
echo "  kubectl get pods -n metallb-system"
echo "  kubectl get ipaddresspool -n metallb-system"
echo "  kubectl get l2advertisement -n metallb-system"

