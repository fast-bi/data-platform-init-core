#!/bin/bash
#

set -o errexit
catch() {
    echo 'catching!'
    if [ "$1" != "0" ]; then
    # error handling goes here
    echo "Error $1 occurred on $2"
    fi
}
trap 'catch $? $LINENO' EXIT

dbt_init_version="v0.5.4"

docker buildx build . \
  --pull \
  --tag europe-central2-docker.pkg.dev/fast-bi-common/bi-platform/tsb-dbt-init-core:${dbt_init_version} \
  --platform linux/amd64 \
  --push

docker buildx build . \
  --pull \
  --tag 4fastbi/data-platform-init-core:dev-latest \
  --tag 4fastbi/data-platform-init-core:dev-v0.1.2 \
  --platform linux/amd64 \
  --push