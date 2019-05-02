#!/usr/bin/env bash

AWS_ACCOUNT_ID=$1
CONTAINER_NAME=$2

if [ -z "$CONTAINER_NAME" ]; then
    echo "Usage $0 [aws_account_id] [container_name]"
    exit 1
fi

if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "Usage $0 [aws_account_id] [container_name]"
    exit 1
fi

$(aws ecr get-login --no-include-email --region us-east-1)
docker build -t $CONTAINER_NAME -f Dockerfile .
docker tag $CONTAINER_NAME:latest $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$CONTAINER_NAME:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/$CONTAINER_NAME:latest