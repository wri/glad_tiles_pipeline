FROM tmaschler/gdal-mapnik:latest
MAINTAINER Thomas Maschler thomas.maschler@wri.org

ENV NAME gfw-glad-pipeline
ENV USER gfw

RUN adduser --shell /bin/bash --disabled-password --gecos "" $USER

COPY requirements.txt /home/$USER/code/
COPY setup.py /home/$USER/code
COPY src /home/$USER/code/src

RUN cd /usr/local/include && ln -s ./ gdal
RUN cd /home/$USER/code && \
    pip3 install -r /home/$USER/code/requirements.txt && \
    pip3 install -e . && \
    g++ src/cpp/add2.cpp -o /usr/bin/add2 -lgdal && \
    g++ src/cpp/build_rgb.cpp -o /usr/bin/build_rgb -lgdal && \
    g++ src/cpp/combine2.cpp -o /usr/bin/combine2 -lgdal && \
    g++ src/cpp/combine3.cpp -o /usr/bin/combine3 -lgdal && \
    g++ src/cpp/reclass.cpp -o /usr/bin/reclass -lgdal