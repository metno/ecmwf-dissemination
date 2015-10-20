import logging
import logging.config
import argparse
import ConfigParser

import ecdiss.recvd
import ecdiss.recvd.daemon


def main():
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
        config_parser.get('ecmwf', 'destination_directory'),
        config_parser.get('modelstatus', 'url'),
        config_parser.get('modelstatus', 'username'),
        config_parser.get('modelstatus', 'password'),
        config_parser.get('modelstatus', 'verify_ssl'),
    )

    daemon.process_directory(spool_directory)
    daemon.main()


if __name__ == '__main__':
    main()
