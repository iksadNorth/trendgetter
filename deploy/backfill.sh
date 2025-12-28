#!/usr/bin/env bash
set -e

NAMESPACE=trendgetter
DEPLOYMENT=airflow-scheduler

# 나머지 모든 인자를 airflow backfill 명령어에 전달
kubectl exec -n "$NAMESPACE" deployment/"$DEPLOYMENT" -- \
  uv run airflow dags backfill "$@"

