#!/bin/bash
echo "building and running containers"
echo `date`

docker rmi $(docker images --filter "dangling=true" -q --no-trunc)
docker-compose build analytics

docker-compose up
