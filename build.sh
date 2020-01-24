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
