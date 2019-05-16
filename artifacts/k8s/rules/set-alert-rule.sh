#!/usr/bin/env bash

curl -X POST \
-H "X-AppsCode-UserID: 1" \
-H "Content-Type: application/json" \
-d "@/home/ac/go/src/github.com/searchlight/ruler/artifacts/k8s/rules/alert-rule-1.json" \
http://localhost:18443/api/prom/rules

curl -X POST \
-H "X-AppsCode-UserID: 2" \
-H "Content-Type: application/json" \
-d "@/home/ac/go/src/github.com/searchlight/ruler/artifacts/k8s/rules/alert-rule-2.json" \
http://localhost:18443/api/prom/rules