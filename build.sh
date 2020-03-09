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

if [ -z "${TAG_BDC_SCRIPTS}" ]; then
  echo "NEW TAG BDC-SCRIPTS:"
  read TAG_BDC_SCRIPTS

  echo
fi

export IMAGE_BDC_SCRIPTS="registry.dpi.inpe.br/brazildatacube/bdc-scripts"
export IMAGE_BDC_SCRIPTS_FULL="${IMAGE_BDC_SCRIPTS}:${TAG_BDC_SCRIPTS}"
echo "IMAGE BDC Scripts :: ${IMAGE_BDC_SCRIPTS_FULL}"

docker-compose build

docker push ${IMAGE_BDC_SCRIPTS_FULL}
