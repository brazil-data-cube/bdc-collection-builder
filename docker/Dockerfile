FROM osgeo/gdal:ubuntu-full-3.2.1

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

RUN apt-get update && \
    apt-get install --yes nano python3-pip git pkg-config cmake libtool autoconf && \
    pip3 install pip --upgrade && \
    rm -rf /var/lib/apt/lists/*

ADD . /app

WORKDIR /app

RUN pip install wheel && \
    pip install pymodis && \
    pip install -e .[all]
