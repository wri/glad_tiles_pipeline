#!/usr/bin/env bash

aws ec2 run-instances \
    --image-id ami-02da3a138888ced85 \
    --instance-type r5d.24xlarge \
    --key-name tmaschler_wri2 \
    --security-group-ids sg-d7a0d8ad sg-6c6a5911 \
    --subnet-id subnet-116d9a4a \
    --user-data file://bootstrap.sh \
    --ebs-optimized \
    --iam-instance-profile Name=gfw_docker_host \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications \
        'ResourceType=instance, Tags=[{Key="Project",Value="Global Forest Watch"},{Key="Project Lead",Value="Thomas Maschler"}, {Key=Pricing,Value="On Demand"},{Key=Job,Value="Glad update"},{Key=Name,Value="Glad update"}]' \
    --count 1 \
    --associate-public-ip-address \
