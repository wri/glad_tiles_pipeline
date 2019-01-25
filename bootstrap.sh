#!/usr/bin/env bash

mkfs.ext4 -E nodiscard /dev/nvme1n1
mkdir -p /mnt/data
mount -o discard /dev/nvme1n1 /mnt/data

mkfs.ext4 -E nodiscard /dev/nvme2n1
mkdir -p /var/lib/docker
mount -o discard /dev/nvme2n1 /var/lib/docker

yum install docker
yum install git

git clone https://github.com/wri/glad_tiles_pipeline.git
cd glad_tiles_pipeline
service docker start
docker build . -t glad-pipeline

docker run --net=host -v /var/run/docker.sock:/var/run/docker.sock lyft/metadataproxy
docker run -e IAM_ROLE=gfw-sync -v /mnt/data:/home/gfw/code/data glad-pipeline glad_pipeline.py --include_russia