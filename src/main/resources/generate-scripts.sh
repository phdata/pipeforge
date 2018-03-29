#!/usr/bin/env bash

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -e|--enviornment)
    ENVIRONMENT="$2"
    shift # past argument
    shift # past value
    ;;
    -c|--config)
    CONFIG="$2"
    shift # past argument
    shift # past value
    ;;
    -t|--template-dir)
    TEMPLATE_DIR="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--directory)
    OUTPUT_DIR="$2"
    shift # past argument
    shift # past value
    ;;
    -p|--pipewrench-dir)
    PIPEWRENCH_DIR="$2"
    shift # past argument
    shift # past value
    ;;
    -v|--virtual-install)
    VIRTUAL_INSTALL="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ $VIRTUAL_INSTALL == "true" ]; then

    source $PIPEWRENCH_DIR/venv/bin/activate
fi

cd $OUTPUT_DIR

pipewrench-merge --env $ENVIRONMENT --conf $CONFIG --pipeline-templates=$TEMPLATE_DIR