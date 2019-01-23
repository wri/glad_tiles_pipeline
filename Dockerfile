FROM geographica/gdal2:latest
MAINTAINER Thomas Maschler thomas.maschler@wri.org

ENV NAME gfw-glad-pipeline
ENV USER gfw
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y install curl git g++ gcc \
    libfreetype6-dev libglib2.0-dev libcairo2-dev \
    libboost-all-dev libicu-dev libxml2 libfreetype6 \
    libfreetype6-dev libjpeg-dev libpng-dev libproj-dev libtiff-dev \
    libcairo2 libcairo2-dev python-cairo python-cairo-dev \
    ttf-unifont ttf-dejavu ttf-dejavu-core ttf-dejavu-extra build-essential \
    libsqlite3-dev python-nose python3-setuptools python3-pip python3-wheel \
    postgresql-10 postgresql-server-dev-10 postgresql-contrib-10 postgresql-10-postgis-2.4
    # gdal-bin python-gdal python3-gdal python-dev

 #  build-essential sudo software-properties-common curl \
 #   libboost-dev libboost-filesystem-dev libboost-program-options-dev \
 #   libboost-python-dev libboost-regex-dev libboost-system-dev libboost-thread-dev \
 #   libicu-dev libtiff5-dev libfreetype6-dev libpng12-dev libxml2-dev libproj-dev \
 #   libsqlite3-dev libgdal-dev libcairo-dev python-cairo-dev postgresql-contrib \
 #   libharfbuzz-dev python3-dev python-dev git python-pip python-setuptools \
 #   python-wheel python3-setuptools python3-pip python3-wheel



# install Harfbuzz
# NOTE extract to /tmp -> -C /tmp/
RUN curl https://www.freedesktop.org/software/harfbuzz/release/harfbuzz-2.3.0.tar.bz2 | tar -xj && \
    cd harfbuzz-2.3.0 && ./configure && make && make install && ldconfig

# install mapnik
RUN apt-get install -y wget
ENV MAPNIK_VERSION 3.0.21
RUN wget https://github.com/mapnik/mapnik/releases/download/v3.0.21/mapnik-v3.0.21.tar.bz2 -P /tmp/
RUN tar xvjf /tmp/mapnik-v3.0.21.tar.bz2 -C /tmp/
RUN cd /tmp/mapnik-v3.0.21 && python scons/scons.py configure
RUN cd /tmp/mapnik-v${MAPNIK_VERSION} && make JOBS=4 && make install JOBS=4
# RUN git clone https://github.com/mapnik/mapnik.git && \
 #   cd mapnik && \
 #   git checkout v3.0.21 && \
 #   git submodule update --init && \
 #   ./configure && make && make install

# Python Bindings
RUN mkdir -p /opt/python-mapnik && curl -L https://github.com/mapnik/python-mapnik/tarball/v3.0.x | tar xz -C /opt/python-mapnik --strip-components=1
RUN cd /opt/python-mapnik && python3 setup.py install && rm -r /opt/python-mapnik/build

RUN addgroup $USER
    #&& adduser --shell /bin/bash --disabled-password --gecos -ingroup $USER $USER

COPY .google /home/$USER/.google
ENV GOOGLE_APPLICATION_CREDENTIALS /home/$USER/.google/earthenginepartners-hansen.json

COPY .aws /home/$USER/.aws

COPY requirements.txt /home/$USER/code/
COPY setup.py /home/$USER/code
COPY src /home/$USER/code/src

RUN apt-get install -y python3-venv
RUN cd /usr/local/include && ln -s ./ gdal
RUN cd /home/$USER/code && python3 -m venv . && \
    pip3 install -r /home/$USER/code/requirements.txt && \
    pip3 install -e . && \
    g++ src/cpp/add2.cpp -o /usr/bin/add2 -lgdal && \
    g++ src/cpp/build_rgb.cpp -o /usr/bin/build_rgb -lgdal && \
    g++ src/cpp/combine2.cpp -o /usr/bin/combine2 -lgdal && \
    g++ src/cpp/combine3.cpp -o /usr/bin/combine3 -lgdal && \
    g++ src/cpp/reclass.cpp -o /usr/bin/reclass -lgdal