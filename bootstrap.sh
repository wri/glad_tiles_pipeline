#!/usr/bin/env bash

yum install docker
yum install git

mkfs.ext4 -E nodiscard /dev/nvme1n1
mkdir -p /mnt/data
mount -o discard /dev/nvme1n1 /mnt/data

git clone https://github.com/wri/glad_tiles_pipeline.git
cd glad_tiles_pipeline
service docker start
docker build . -t glad-pipeline

docker run -v /mnt/data:/home/gfw/code/data glad-pipeline glad_pipeline.py --include_russia