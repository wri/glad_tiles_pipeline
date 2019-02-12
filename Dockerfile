FROM tmaschler/gdal-mapnik-tippecanoe:latest
MAINTAINER Thomas Maschler thomas.maschler@wri.org

ENV NAME gfw-glad-pipeline
ENV USER gfw

RUN adduser --shell /bin/bash --disabled-password --gecos "" $USER

COPY requirements.txt /home/$USER/
COPY setup.py /home/$USER/
COPY glad /home/$USER/glad
COPY cpp /home/$USER/cpp
COPY .aws  /home/$USER/.aws
COPY .google  /home/$USER/.google

ENV AWS_SHARED_CREDENTIALS_FILE /home/$USER/.aws/credentials
ENV AWS_CONFIG_FILE /home/$USER/.aws/config
ENV GOOGLE_APPLICATION_CREDENTIALS /home/$USER/.google/earthenginepartners-hansen.json

RUN cd /usr/local/include && ln -s ./ gdal
RUN cd /home/$USER && \
    pip3 install -r /home/$USER/requirements.txt && \
    pip3 install -e . && \
    g++ cpp/add2.cpp -o /usr/bin/add2 -lgdal && \
    g++ cpp/build_rgb.cpp -o /usr/bin/build_rgb -lgdal && \
    g++ cpp/combine2.cpp -o /usr/bin/combine2 -lgdal && \
    g++ cpp/combine3.cpp -o /usr/bin/combine3 -lgdal && \
    g++ cpp/reclass.cpp -o /usr/bin/reclass -lgdal
