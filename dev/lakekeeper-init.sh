#!/bin/sh
# Bootstrap Lakekeeper and create the `glossql` warehouse over the
# SeaweedFS bucket — once; a second run finds both and does nothing.
# SeaweedFS speaks S3 without STS, so the warehouse holds its keys and
# vends none of them: the server reads them from AWS_* (dev/README.md).
set -e
base=http://lakekeeper:8181/management/v1
until curl -sf "$base/info" >/dev/null; do sleep 1; done
if ! curl -sf "$base/info" | grep -q '"bootstrapped":true'; then
  curl -sf -X POST "$base/bootstrap" -H 'Content-Type: application/json' \
    -d '{"accept-terms-of-use": true}'
  echo "bootstrapped"
fi
if ! curl -sf "$base/warehouse" | grep -q '"name":"glossql"'; then
  # Creating the warehouse writes a test object through the S3 gateway,
  # which may still be coming up behind the bucket: a few tries.
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl -sf -X POST "$base/warehouse" -H 'Content-Type: application/json' -d '{
      "warehouse-name": "glossql",
      "storage-profile": {"type": "s3", "bucket": "lake", "region": "local-01",
        "path-style-access": true, "endpoint": "http://seaweedfs:8333",
        "sts-enabled": false, "flavor": "s3-compat"},
      "storage-credential": {"type": "s3", "credential-type": "access-key",
        "aws-access-key-id": "seaweed-dev", "aws-secret-access-key": "seaweed-secret"}}' >/dev/null; then
      echo "warehouse glossql created"
      exit 0
    fi
    sleep 2
  done
  echo "the warehouse could not be created" >&2
  exit 1
fi
