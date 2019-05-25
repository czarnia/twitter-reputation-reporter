#!/bin/bash

docker build -t reporter-init -f src/init/Dockerfile .
docker build -t reporter-filter-parser -f src/filter_parser/Dockerfile .
docker build -t reporter-analyzer -f src/analyzer/Dockerfile .
docker build -t reporter-user-reducer -f src/user_reducer/Dockerfile .
docker build -t reporter-date-reducer -f src/date_reducer/Dockerfile .
docker build -t reporter-date-aggregator -f src/date_agregator/Dockerfile .

docker-compose up -d reporter-init reporter-filter-parser reporter-analyzer reporter-user-reducer reporter-date-reducer reporter-date-aggregator
