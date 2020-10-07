FROM registry.dpi.inpe.br/brazildatacube/geo:0.2

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

ADD . /app

WORKDIR /app

RUN pip3 install pip --upgrade && \
    pip install wheel && \
    pip install -e .[all] --use-feature=2020-resolver
