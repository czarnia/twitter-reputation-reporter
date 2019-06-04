#!/bin/bash

python3 docker_compose_generator.py --filter-parser-workers=2 --analyzer-workers=5 --user-reducer-workers=2 --date-reducer-workers=2

docker build -t reporter-init -f src/init/Dockerfile .
docker build -t reporter-filter-parser -f src/filter_parser/Dockerfile .
docker build -t reporter-analyzer -f src/analyzer/Dockerfile .
docker build -t reporter-user-reducer -f src/user_reducer/Dockerfile .
docker build -t reporter-date-reducer -f src/date_reducer/Dockerfile .
docker build -t reporter-date-aggregator -f src/date_aggregator/Dockerfile .

docker-compose up
