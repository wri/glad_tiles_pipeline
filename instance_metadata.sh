#!/usr/bin/env bash

curl http://169.254.169.254/latest/meta-data/iam/security-credentials/$IAM_ROLE | jq '.' | export AWS_ACCESS_KEY_ID=.AccessKeyId

