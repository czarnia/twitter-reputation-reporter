import argparse

def generate_env_file(args):
    with open("config.env", "w") as config_file:
        config_file.write("RABBITMQ_HOST=rabbitmq\n")
        config_file.write("FILTER_PARSER_WORKERS={}\n".format(args.filter_parser_workers))
        config_file.write("ANALYZER_WORKERS={}\n".format(args.analyzer_workers))
        config_file.write("USER_REDUCER_WORKERS={}\n".format(args.user_reducer_workers))
        config_file.write("DATE_REDUCER_WORKERS={}\n".format(args.user_reducer_workers))

        config_file.write("TWITS_FILE=/twitter_reporter/reports/{}\n".format(args.twits_file))
        config_file.write("LOGS_FILE=/twitter_reporter/reports/{}".format(args.logs_file))

def write_header(file):
    file.write("version: '2'\nservices:\n")

def write_rabbit_service(file):
    file.write("  rabbitmq:\n    image: rabbitmq:3.7.14-management\n    ports:\n      - 15672:15672\n    healthcheck:\n        test: [\"CMD\", \"curl\", \"-f\", \"http://localhost:15672\"]\n        interval: 30s\n        timeout: 10s\n        retries: 5\n\n")

def write_init_service(file):
    file.write("  reporter-init:\n    image: reporter-init\n    env_file:\n      - config.env\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    links:\n      - rabbitmq\n    depends_on:\n      - rabbitmq\n\n")

def write_filter_parser_service(file, service_id):
    file.write("  reporter-filter-parser-{}:\n    image: reporter-filter-parser\n    env_file:\n      - config.env\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    links:\n      - rabbitmq\n    depends_on:\n      - rabbitmq\n\n".format(service_id))

def write_analyzer_service(file, service_id):
    file.write("  reporter-analyzer-{}:\n    image: reporter-analyzer\n    env_file:\n      - config.env\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    links:\n      - rabbitmq\n    depends_on:\n      - rabbitmq\n\n".format(service_id))

def write_user_reducer_service(file, service_id):
    file.write("  reporter-user-reducer-{}:\n    image: reporter-user-reducer\n    env_file:\n      - config.env\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    links:\n      - rabbitmq\n    depends_on:\n      - rabbitmq\n\n".format(service_id))

def write_date_reducer_service(file, service_id):
    file.write("  reporter-date-reducer-{}:\n    image: reporter-date-reducer\n    env_file:\n      - config.env\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    links:\n      - rabbitmq\n    depends_on:\n      - rabbitmq\n\n".format(service_id))

def write_date_aggregator_service(file):
    file.write("  reporter-date-aggregator:\n    image: reporter-date-aggregator\n    env_file:\n      - config.env\n    volumes:\n      - ./reports:/twitter_reporter/reports\n    links:\n      - rabbitmq\n    depends_on:\n      - rabbitmq\n\n")

def generate_docker_compose_file(args):
    with open("docker-compose.yml", "w") as docker_compose_file:
        write_header(docker_compose_file)
        write_rabbit_service(docker_compose_file)
        write_init_service(docker_compose_file)

        for i in range(args.filter_parser_workers):
            write_filter_parser_service(docker_compose_file, i+1)

        for i in range(args.analyzer_workers):
            write_analyzer_service(docker_compose_file, i+1)

        for i in range(args.user_reducer_workers):
            write_user_reducer_service(docker_compose_file, i+1)

        for i in range(args.date_reducer_workers):
            write_date_reducer_service(docker_compose_file, i+1)

        write_date_aggregator_service(docker_compose_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--filter-parser-workers", help="number of filter parser workers", default=1, type=int)
    parser.add_argument("--analyzer-workers", help="number of analyzer workers", default=1, type=int)
    parser.add_argument("--user-reducer-workers", help="number of user reducer workers", default=1, type=int)
    parser.add_argument("--date-reducer-workers", help="number of date reducer workers", default=1, type=int)

    parser.add_argument("--twits-file", help="file from where to read the twits", default="sample.csv")
    parser.add_argument("--logs-file", help="log file to write to", default="logs")

    args = parser.parse_args()

    generate_env_file(args)
    generate_docker_compose_file(args)

