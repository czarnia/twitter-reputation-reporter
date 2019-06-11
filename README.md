# twitter-reputation-reporter

Repository for the 2st project of "Sistemas Distribuidos I". It features a twitter reputation reporter (see bellow).

## Installation

Just download the repo and you are good to go! Python3, docker and docker-compose required.

### Configuration

All the configuration is handled by the `docker_compose_generator` python script, the following options are provided:

```
--filter-parser-workers=<number of filter parser workers>
--analyzer-workers=<number of analyzer workers>
--user-reducer-workers=<number of user reducer workers>
--date-reducer-workers=<number of date reducer workers>
--twits-file=<file from where to read the twits, the file should be located in the "reports" folder>
--logs-file=<log file to write to, the file should be located in the "reports" folder>
```

## Usage

The reporter can be started by using the following comand in the root dir of this repository.

``` bash
sh twitter-reputation-reporter-up.sh
```

The reports that this system generates are two, one for the users that leaved more than three negative twits and another one for the number of positive and negative twits for each day. Both of them will be placed in the "reports" folder.

It is possible to change the configuration by changing the call to the `docker_compose_generator` in the first line of the `twitter-reputation-reporter-up` bash script as stated before in the "Configuration" section. 
