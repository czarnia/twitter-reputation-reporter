#!/bin/bash

docker build -t twitter-reporter -f src/Dockerfile .
docker-compose up
