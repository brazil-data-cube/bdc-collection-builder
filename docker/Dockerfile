#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2024 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

ARG BASE_IMAGE=osgeo/gdal:ubuntu-full-3.2.1
FROM ${BASE_IMAGE}

ARG GIT_COMMIT=unknown

# Image metadata
LABEL "org.repo.maintainer"="Brazil Data Cube <brazildatacube@inpe.br>"
LABEL "org.repo.title"="Docker image for Data Collection Builder"
LABEL "org.repo.description"="Docker image for Data Collection Builder Server for Brazil Data Cube."
LABEL "org.repo.git_commit"="${GIT_COMMIT}"
LABEL "org.repo.licenses"="GPLv3"

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

# Build arguments
ARG BDC_BUILDER_INSTALL_PATH="/opt/bdc-collection-builder"

RUN apt-get update && \
    apt-get install --yes nano python3-pip git pkg-config cmake libtool autoconf && \
    pip3 install pip --upgrade && \
    rm -rf /var/lib/apt/lists/*

COPY . ${BDC_BUILDER_INSTALL_PATH}
WORKDIR ${BDC_BUILDER_INSTALL_PATH}

RUN pip install -U wheel setuptools && \
    pip install pymodis --no-cache && \
    pip install .[amqp] --no-cache
