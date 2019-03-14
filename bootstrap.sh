#!/usr/bin/env bash

mkfs.ext4 -E nodiscard /dev/nvme1n1
mkdir -p /mnt/data
mount -o discard /dev/nvme1n1 /mnt/data

mkfs.ext4 -E nodiscard /dev/nvme2n1
mkdir -p /var/lib/docker
mount -o discard /dev/nvme2n1 /var/lib/docker

mkfs.ext4 -E nodiscard /dev/nvme3n1
mkdir -p /mnt/log
mount -o discard /dev/nvme3n1 /mnt/log


yum -y install docker git jq htop

cd /home/ec2-user

git clone https://github.com/wri/glad_tiles_pipeline.git
cd glad_tiles_pipeline
git checkout feature/shutdown_privileged
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

[GFWPro_gfwpro-raster-data_remote]
aws_access_key_id = $pro_id
aws_secret_access_key = $pro_secret
aws_session_token = $pro_token

EOL

cat >.aws/config <<EOL

[default]
output = json
region = us-east-1

[profile GFWPro_gfwpro-raster-data_remote]
output = json
region = us-east-1
EOL

cat >.google/earthenginepartners-hansen.json <<EOL
$secret
EOL

service docker start
docker build . -t glad-pipeline

while sleep 30; do [ -f /mnt/log/glad/done ] && shutdown -h now; done &

docker run -d --ulimit nofile=4096:4096 -e IAM_ROLE=gfw-sync -v /mnt/data:/usr/data -v /mnt/log:/var/log glad-pipeline glad_pipeline.py -w 35 --env test --shutdown
