#!/bin/bash

python3 docker_compose_generator.py --filter-parser-workers=1 --analyzer-workers=1 --user-reducer-workers=1 --date-reducer-workers=1 --twits-file=testing.csv

docker build -t middleware-base -f src/middleware/Dockerfile ./src/middleware
docker build -t rabbitmq-healthy -f src/middleware/rabbitmq/Dockerfile ./src/middleware/rabbitmq

docker build -t reporter-init -f src/init/Dockerfile ./src/init
docker build -t reporter-filter-parser -f src/filter_parser/Dockerfile ./src/filter_parser
docker build -t reporter-analyzer -f src/analyzer/Dockerfile ./src/analyzer
docker build -t reporter-user-reducer -f src/user_reducer/Dockerfile ./src/user_reducer
docker build -t reporter-date-reducer -f src/date_reducer/Dockerfile ./src/date_reducer
docker build -t reporter-date-aggregator -f src/date_aggregator/Dockerfile ./src/date_aggregator

docker-compose up
docker-compose down

find . -name "*.env" -type f -delete
rm docker-compose.yml
