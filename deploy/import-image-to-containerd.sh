#!/bin/bash
set -e

DOCKER_IMAGE_NAME=iksadnorth/airflow:1.0.0
TAR_FILE=/tmp/airflow-image-$(date +%s).tar

docker save "$DOCKER_IMAGE_NAME" -o "$TAR_FILE"
sudo ctr -n k8s.io images import "$TAR_FILE"
rm -f "$TAR_FILE"

