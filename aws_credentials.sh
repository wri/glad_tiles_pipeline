#!/usr/bin/env bash

meta=$(aws sts assume-role --role-arn arn:aws:iam::838255262149:role/gfw_sync --role-session-name GFWsync)
default_id=$(echo $meta | jq '.Credentials.AccessKeyId'  | sed -e 's/^"//' -e 's/"$//')
default_secret=$(echo $meta | jq '.Credentials.SecretAccessKey'  | sed -e 's/^"//' -e 's/"$//')
default_token=$(echo $meta | jq '.Credentials.SessionToken'  | sed -e 's/^"//' -e 's/"$//')

meta=$(aws sts assume-role --role-arn arn:aws:iam::617001639586:role/GFWPro_gfwpro-raster-data_remote --role-session-name GFWpro)
pro_id=$(echo $meta | jq '.Credentials.AccessKeyId'  | sed -e 's/^"//' -e 's/"$//')
pro_secret=$(echo $meta | jq '.Credentials.SecretAccessKey'  | sed -e 's/^"//' -e 's/"$//')
pro_token=$(echo $meta | jq '.Credentials.SessionToken'  | sed -e 's/^"//' -e 's/"$//')

cat >credentials <<EOL
[default]
aws_access_key_id = $default_id
aws_secret_access_key = $default_secret
aws_session_token = $default_token

[profile gfwpro]
aws_access_key_id = $pro_id
aws_secret_access_key = $pro_secret
aws_session_token = $pro_token

EOL

cat >config <<EOL

[default]
output = json
region = us-east-1

[gfwpro]
output = json
region = us-east-1
EOL