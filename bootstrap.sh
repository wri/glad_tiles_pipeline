#!/usr/bin/env bash

mkfs.ext4 -E nodiscard /dev/nvme1n1
mkdir -p /mnt/data
mount -o discard /dev/nvme1n1 /mnt/data

mkfs.ext4 -E nodiscard /dev/nvme2n1
mkdir -p /var/lib/docker
mount -o discard /dev/nvme2n1 /var/lib/docker

yum -y install docker git jq

git clone https://github.com/wri/glad_tiles_pipeline.git
cd glad_tiles_pipeline
mkdir .aws
mkdir .google

meta=$(aws sts assume-role --role-arn arn:aws:iam::838255262149:role/gfw_sync --role-session-name GFWsync)
default_id=$(echo $meta | jq '.Credentials.AccessKeyId'  | sed -e 's/^"//' -e 's/"$//')
default_secret=$(echo $meta | jq '.Credentials.SecretAccessKey'  | sed -e 's/^"//' -e 's/"$//')
default_token=$(echo $meta | jq '.Credentials.SessionToken'  | sed -e 's/^"//' -e 's/"$//')

meta=$(aws sts assume-role --role-arn arn:aws:iam::617001639586:role/GFWPro_gfwpro-raster-data_remote --role-session-name GFWpro)
pro_id=$(echo $meta | jq '.Credentials.AccessKeyId'  | sed -e 's/^"//' -e 's/"$//')
pro_secret=$(echo $meta | jq '.Credentials.SecretAccessKey'  | sed -e 's/^"//' -e 's/"$//')
pro_token=$(echo $meta | jq '.Credentials.SessionToken'  | sed -e 's/^"//' -e 's/"$//')

secret=$(aws secretsmanager get-secret-value --secret-id google_cloud/gfw_sync --region us-east-1 | jq '.SecretString'  | sed -e 's/^"//' -e 's/"$//' -e 's/\\"/"/g' -e 's/\\\\n/\\n/g')

cat >.aws/credentials <<EOL
[default]
aws_access_key_id = $default_id
aws_secret_access_key = $default_secret
aws_session_token = $default_token

[profile gfwpro]
aws_access_key_id = $pro_id
aws_secret_access_key = $pro_secret
aws_session_token = $pro_token

EOL

cat >.aws/config <<EOL

[default]
output = json
region = us-east-1

[gfwpro]
output = json
region = us-east-1
EOL

cat >.google/earthenginepartners-hansen.json <<EOL
$secret
EOL

service docker start
docker build . -t glad-pipeline

docker run -it -e IAM_ROLE=gfw-sync -v /mnt/data:/usr/data glad-pipeline glad_pipeline.py -w 40