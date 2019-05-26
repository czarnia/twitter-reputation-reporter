# twitter-reputation-reporter

Repository for the 2st project of "Sistemas Distribuidos I". It features a twitter reputation reporter (see bellow).

## Installation

Just download the repo and you are good to go! Docker and docker-compose required.

### Configuration

In the config file the following options are provided:

```
RABBITMQ_HOST=<host of the rabbitmq server>
FILTER_PARSER_WORKERS=<number of filter parser workers>
ANALYZER_WORKERS=<number of analyzer workers>
USER_REDUCER_WORKERS=<number of user reducer workers>
DATE_REDUCER_WORKERS=<number of date reducer workers>
TWITS_FILE=<the twits file to report from>
LOGS_FILE=<logs file to store logs>
```

## Usage

The reporter can be started by using the following comand in the root dir of this repository.

``` bash
sh twitter-reputation-reporter-up.sh
```

The twits file should be located in the "reports" folder and its path written under the config.env file (a sample configuration is provided). The reports that this system generates are two, one for the users that leaved more than three negative twits and another one for the number of positive and negative twits for each day. Both of them will be placed in the "reports" folder.

Note that it is necessary to also have a rabbitmq server running, the rabbitmq host can be defined in the config file. It is possible to start a dockerized rabbitmq server using the following command from the root dir of this repository:

```
docker-compose up rabbitmq
```