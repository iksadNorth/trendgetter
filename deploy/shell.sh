#!/usr/bin/env bash
set -e

NAMESPACE=trendgetter
DEPLOYMENT=airflow-scheduler

POD=$(kubectl get pods -n "$NAMESPACE" \
  -l app=$DEPLOYMENT \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -it -n "$NAMESPACE" "$POD" -- uv run python
