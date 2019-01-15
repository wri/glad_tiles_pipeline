FROM geographica/gdal2:latest
MAINTAINER Thomas Maschler thomas.maschler@wri.org

ENV NAME gfw-glad-pipeline
ENV USER gfw

RUN apt-get -y update && apt-get -y install curl g++

RUN addgroup $USER # \
    #&& adduser --shell /bin/bash --disabled-password --gecos -ingroup $USER $USER

COPY .google .google

RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    export GOOGLE_APPLICATION_CREDENTIALS=".google/earthenginepartners-hansen.json" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get -y update && apt-get -y install google-cloud-sdk && \
    # gcloud init --console-only && \
    gcloud auth activate-service-account --key-file .google/earthenginepartners-hansen.json && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true
    # gcloud config set metrics/environment github_docker_image

COPY requirements.txt /home/docker/code/
COPY src /home/docker/code/src



RUN python3 -m venv . && \
    pip install -r /home/docker/code/requirements.txt && \
    pip install -e src && \
    g++ src/cpp/add2.cpp -o /usr/bin/add2 -lgdal && \
    g++ src/cpp/build_rgb.cpp -o /usr/bin/build_rgb -lgdal && \
    g++ src/cpp/combine2.cpp -o /usr/bin/combine2 -lgdal && \
    g++ src/cpp/combine3.cpp -o /usr/bin/combine3 -lgdal && \
    g++ src/cpp/reclass.cpp -o /usr/bin/reclass -lgdal