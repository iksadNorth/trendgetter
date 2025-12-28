#!/bin/bash
set -e

IMAGE_NAME="superset:3.1.1-pg"
DOCKERFILE_PATH="superset_config/Dockerfile"

echo "Building Docker image: $IMAGE_NAME"
docker build -t $IMAGE_NAME -f $DOCKERFILE_PATH .

echo "Saving Docker image to tar file..."
docker save $IMAGE_NAME -o superset-image.tar

echo "Importing image to containerd..."
sudo ctr -n=k8s.io images import superset-image.tar

echo "Cleaning up temporary tar file..."
rm -f superset-image.tar

echo "Verifying image in containerd..."
sudo ctr -n=k8s.io images list | grep superset

echo "Done! Image $IMAGE_NAME is now available in containerd."

