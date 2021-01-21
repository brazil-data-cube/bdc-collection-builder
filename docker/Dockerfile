FROM registry.dpi.inpe.br/brazildatacube/geo:0.2

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

ADD . /app

WORKDIR /app

RUN apt-get update && \
    apt-get install --yes nano && \
    pip3 install pip --upgrade && \
    rm -rf /var/lib/apt/lists/* && \
    pip install wheel && \
    pip install -e .[all]
