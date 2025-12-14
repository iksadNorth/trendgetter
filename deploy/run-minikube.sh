#!/bin/bash
set -e

PWD_ROOT_PATH=$(pwd)
DOCKER_IMAGE_NAME=iksadnorth/airflow:1.0.0
echo "PWD_ROOT_PATH: $PWD_ROOT_PATH"
echo "DOCKER_IMAGE_NAME: $DOCKER_IMAGE_NAME"

echo "Minikube 시작 중..."
minikube stop || true
minikube delete || true
minikube start --mount=true --mount-string="$PWD_ROOT_PATH/:$PWD_ROOT_PATH/"

echo "Minikube Docker 환경 설정 중..."
eval $(minikube docker-env)

# 프로젝트 루트로 이동
cd "$PWD_ROOT_PATH"

echo "Docker 이미지 빌드 중"
docker build -t $DOCKER_IMAGE_NAME .

echo "minikube tunnel 백그라운드 실행"
minikube tunnel >./minikube-tunnel.log 2>&1 &
