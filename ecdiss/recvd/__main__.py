import sys
import logging
import logging.config
import argparse
import traceback
import ConfigParser

import ecdiss.recvd
import ecdiss.recvd.daemon


def run():
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--config', action='store', required=True,
                                 help='Path to configuration file')

    args = argument_parser.parse_args()

    logging.config.fileConfig(args.config)

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(args.config)

    spool_directory = config_parser.get('ecmwf', 'spool_directory')
    watcher = ecdiss.recvd.DirectoryWatch(spool_directory)
    daemon = ecdiss.recvd.daemon.Daemon(
        watcher,
        config_parser.get('hosting', 'base_url'),
        int(config_parser.get('hosting', 'file_lifetime')),
        config_parser.get('hosting', 'service_backend'),
        config_parser.get('ecmwf', 'destination_directory'),
        config_parser.get('modelstatus', 'url'),
        config_parser.get('modelstatus', 'username'),
        config_parser.get('modelstatus', 'api_key'),
        config_parser.get('modelstatus', 'verify_ssl'),
    )

    daemon.process_directory(spool_directory)
    daemon.main()

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
