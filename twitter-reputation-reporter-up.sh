#!/bin/bash

cd ./src

docker build -t twitter_reporter .

cd ../
docker-compose up
