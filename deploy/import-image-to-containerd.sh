#!/bin/bash
set -e

DOCKER_IMAGE_NAME=iksadnorth/airflow:1.0.0
TAR_FILE=./airflow-image-$(date +%s).tar

echo "Docker 이미지 빌드 중..."
docker build -t "$DOCKER_IMAGE_NAME" .

echo "이미지를 tar 파일로 저장 중..."
docker save "$DOCKER_IMAGE_NAME" -o "$TAR_FILE"

echo "containerd로 이미지 import 중..."
sudo ctr -n k8s.io images import "$TAR_FILE"

echo "임시 tar 파일 삭제 중..."
rm -f "$TAR_FILE"

echo "완료!"

