#!/usr/bin/env bash
#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2022 INPE.
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

#Build docker images


echo
echo "BUILD STARTED"
echo

if [ -z "${TAG_BDC_COLLECTION_BUILDER}" ]; then
  echo "NEW TAG BDC-COLLECTION-BUILDER:"
  read TAG_BDC_COLLECTION_BUILDER

  echo
fi

export IMAGE_BDC_COLLECTION_BUILDER="registry.dpi.inpe.br/brazil-data-cube/bdc-collection-builder"
export IMAGE_BDC_COLLECTION_BUILDER_FULL="${IMAGE_BDC_COLLECTION_BUILDER}:${TAG_BDC_COLLECTION_BUILDER}"
echo "IMAGE BDC COLLECTION-BUILDER :: ${IMAGE_BDC_COLLECTION_BUILDER_FULL}"

docker-compose build
docker tag ${IMAGE_BDC_COLLECTION_BUILDER}:latest ${IMAGE_BDC_COLLECTION_BUILDER_FULL}
docker tag ${IMAGE_BDC_COLLECTION_BUILDER}-atm:latest ${IMAGE_BDC_COLLECTION_BUILDER}-atm:${TAG_BDC_COLLECTION_BUILDER}
docker push ${IMAGE_BDC_COLLECTION_BUILDER_FULL}
docker push ${IMAGE_BDC_COLLECTION_BUILDER}-atm:${TAG_BDC_COLLECTION_BUILDER}