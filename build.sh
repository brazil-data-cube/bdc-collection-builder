#!/usr/bin/env bash
#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
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

export IMAGE_BDC_COLLECTION_BUILDER="registry.dpi.inpe.br/brazildatacube/bdc-collection-builder"
export IMAGE_BDC_COLLECTION_BUILDER_FULL="${IMAGE_BDC_COLLECTION_BUILDER}:${TAG_BDC_COLLECTION_BUILDER}"
echo "IMAGE BDC COLLECTION-BUILDER :: ${IMAGE_BDC_COLLECTION_BUILDER_FULL}"

docker-compose build
docker tag ${IMAGE_BDC_COLLECTION_BUILDER}:latest ${IMAGE_BDC_COLLECTION_BUILDER_FULL}
docker push ${IMAGE_BDC_COLLECTION_BUILDER_FULL}
