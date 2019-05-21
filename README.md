# twitter-reputation-reporter

Repository for the 2st project of "Sistemas Distribuidos I". It features a twitter reputation reporter (see bellow).

## Installation

Just download the repo and you are good to go! Docker and docker-compose required.

### Configuration

The number of nodes for each step in the reporter pipeline can be configured in the config.env file (see the docs for further information).

## Usage

The reporter can be started by using the following comand in the root dir of this repository.

``` bash
sh twitter-reputation-reporter-up.sh
```

The twits file should be located in the "reports" folder and its path written under the config.env file (a sample configuration is provided). The reports that this system generates are two, one for the users that leaved more than three negative twits and another one for the number of positive and negative twits for each day. Both of them will be placed in the "reports" folder. 
