FROM alpine:3.6

MAINTAINER Ricardo Cartaxo <ricardo.cartaxo@inpe.br> 

RUN apk --update  --repository http://dl-4.alpinelinux.org/alpine/edge/community add \
    bash \
    git \
    curl \
    ca-certificates \
    bzip2 \
    unzip \
    sudo \
    libstdc++ \
    glib \
    libxext \
    libxrender \
    tini \ 
    && curl -L "https://github.com/andyshinn/alpine-pkg-glibc/releases/download/2.25-r0/glibc-2.25-r0.apk" -o /tmp/glibc.apk \
    && curl -L "https://github.com/andyshinn/alpine-pkg-glibc/releases/download/2.25-r0/glibc-bin-2.25-r0.apk" -o /tmp/glibc-bin.apk \
    && curl -L "https://github.com/andyshinn/alpine-pkg-glibc/releases/download/2.25-r0/glibc-i18n-2.25-r0.apk" -o /tmp/glibc-i18n.apk \
    && apk add --allow-untrusted /tmp/glibc*.apk \
    && /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib \
    && /usr/glibc-compat/bin/localedef -i en_US -f UTF-8 en_US.UTF-8 \
    && rm -rf /tmp/glibc*apk /var/cache/apk/*

# Configure environment
ENV CONDA_DIR=/opt/conda CONDA_VER=4.3.14
ENV PATH=$CONDA_DIR/bin:$PATH SHELL=/bin/bash LANG=C.UTF-8

# Install conda
RUN mkdir -p $CONDA_DIR && \
    echo export PATH=$CONDA_DIR/bin:'$PATH' > /etc/profile.d/conda.sh && \
    curl https://repo.continuum.io/miniconda/Miniconda3-${CONDA_VER}-Linux-x86_64.sh  -o mconda.sh && \
    /bin/bash mconda.sh -f -b -p $CONDA_DIR && \
    rm mconda.sh && \
    $CONDA_DIR/bin/conda install --yes conda==${CONDA_VER}
# Install matplotlib and scikit-image without Qt
RUN conda update -y python conda && \
  conda install  -c conda-forge -y --no-deps \
  matplotlib \
  cycler \
  freetype \
  libpng \
  pyparsing \
  pytz \
  python-dateutil \
  scikit-image \
  scikit-learn \
  networkx \
  pillow \
  six \
  && conda clean -tipsy

RUN conda install -y \
  pip \
  setuptools \
#  notebook \
  ipywidgets \
#  terminado \
  psutil \
  numpy \
  scipy \
  pandas \
  bokeh \
#  scikit-learn \
#  statsmodels \
  gdal \
  gunicorn \
  flask \
  redis \
  sqlalchemy \
  mysqlclient \
  requests \
 && conda clean -tipsy
 
 RUN pip install --upgrade pip && pip install numpngw h5py
 
# Set the working directory to /app
WORKDIR /app

# Create config directory  /config
RUN mkdir /ds_config

# Setting environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=$PYTHONPATH:/ds_include

# Expose the Flask port
EXPOSE 5021

# Run the maestro application
CMD [ "python3", "soloist_wsgi.py" ]
