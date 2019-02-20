FROM tmaschler/gdal-mapnik:latest
MAINTAINER Thomas Maschler thomas.maschler@wri.org

ENV NAME gfw-glad-pipeline
ENV SRC_PATH /usr/src/glad
ENV SECRETS_PATH /usr/secrets

RUN mkdir -p $SRC_PATH
RUN mkdir -p $SECRETS_PATH

COPY requirements.txt $SRC_PATH
COPY setup.py $SRC_PATH
COPY glad $SRC_PATH/glad
COPY cpp $SRC_PATH/cpp
COPY .aws  $SECRET_PATH/.aws
COPY .google  $SECRET_PATH/.google

ENV AWS_SHARED_CREDENTIALS_FILE $SECRET_PATH/.aws/credentials
ENV AWS_CONFIG_FILE $SECRET_PATH/.aws/config
ENV GOOGLE_APPLICATION_CREDENTIALS $SECRET_PATH/.google/earthenginepartners-hansen.json

RUN cd /usr/local/include && ln -s ./ gdal
RUN cd $SRC_PATH && \
    pip3 install -r $SRC_PATH/requirements.txt && \
    pip3 install -e . && \
    g++ cpp/add2.cpp -o /usr/bin/add2 -lgdal && \
    g++ cpp/build_rgb.cpp -o /usr/bin/build_rgb -lgdal && \
    g++ cpp/combine2.cpp -o /usr/bin/combine2 -lgdal && \
    g++ cpp/combine3.cpp -o /usr/bin/combine3 -lgdal && \
    g++ cpp/reclass.cpp -o /usr/bin/reclass -lgdal
