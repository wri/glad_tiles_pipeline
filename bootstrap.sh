#!/usr/bin/env bash

sudo yum install docker
sudo yum install git

sudo mkfs.ext4 -E nodiscard /dev/nvme1n1
sudo mkdir -p /mnt/data
sudo mount -o discard /dev/nvme1n1 /mnt/data

git clone https://github.com/wri/glad_tiles_pipeline.git
cd glad_tiles_pipeline
sudo service docker start
sudo docker build . -t glad-pipeline

sudo docker run -it -v /mnt/data:/home/gfw/code/data glad-pipeline glad_pipeline.py