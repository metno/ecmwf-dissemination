import sys
import logging
import logging.config
import argparse
import traceback
import ConfigParser

import ecdiss.recvd
import ecdiss.recvd.daemon

import productstatus.api


def prepare():
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--config', action='store', required=True,
                                 help='Path to configuration file')

    args = argument_parser.parse_args()

    logging.config.fileConfig(args.config)
    logging.info('Starting up ECMWF dissemination receiver.')

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(args.config)

    spool_directory = config_parser.get('ecmwf', 'spool_directory')
    destination_directory = config_parser.get('ecmwf', 'destination_directory')

    checkpoint_file = config_parser.get('ecdiss', 'checkpoint_file')
    checkpoint = ecdiss.recvd.daemon.Checkpoint(checkpoint_file)

    productstatus_api = productstatus.api.Api(
        config_parser.get('productstatus', 'url'),
        username=config_parser.get('productstatus', 'username'),
        api_key=config_parser.get('productstatus', 'api_key'),
        verify_ssl=config_parser.getboolean('productstatus', 'verify_ssl'),
    )
    productstatus_service_backend = \
        productstatus_api.servicebackend[config_parser.get('hosting', 'service_backend')]
    productstatus_source = \
        productstatus_api.institution[config_parser.get('productstatus', 'source_uuid')]

    publisher = ecdiss.recvd.daemon.DatasetPublisher(
        checkpoint,
        config_parser.get('hosting', 'base_url'),
        int(config_parser.get('hosting', 'file_lifetime')),
        productstatus_service_backend,
        productstatus_source,
        destination_directory,
        productstatus_api,
    )

    def process_incomplete_checkpoints():
        publisher.process_incomplete_checkpoints([destination_directory, spool_directory])

    process_incomplete_checkpoints()
    publisher.process_directory(spool_directory)

    watcher = ecdiss.recvd.daemon.DirectoryDaemon(
        spool_directory,
        callback_new_file=publisher.process_file,
        callback_timeout=process_incomplete_checkpoints,
    )

    return watcher


def run():
    try:
        watcher = ecdiss.recvd.daemon.retry_n(prepare, give_up=-1)

        logging.info('ECMWF dissemination receiver daemon waiting for events.')
        watcher.run()
    except KeyboardInterrupt:
        pass

    logging.info('ECMWF dissemination receiver daemon terminating.')

    return 0


def main():
    try:
        exit_code = run()
    except Exception, e:
        logging.critical("Fatal error: %s" % e)
        exception = traceback.format_exc().split("\n")
        logging.debug("***********************************************************")
        logging.debug("Uncaught exception during program execution. THIS IS A BUG!")
        logging.debug("***********************************************************")
        for line in exception:
            logging.debug(line)
        exit_code = 255

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
