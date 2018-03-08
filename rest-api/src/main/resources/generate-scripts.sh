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
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

cd $OUTPUT_DIR

pipewrench-merge --env $ENVIRONMENT --conf $CONFIG --pipeline-templates=$TEMPLATE_DIR