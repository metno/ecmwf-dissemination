import logging
import argparse

import ecdiss.recvd
import ecdiss.recvd.daemon


def setup_logging():
    logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s', level=logging.DEBUG)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', action='store', required=True,
                        help='Directory to watch for input files')
    parser.add_argument('--output', action='store', required=True,
                        help='Move valid input file to this directory')
    parser.add_argument('--modelstatus', action='store', required=True,
                        help='URL to Modelstatus service')
    parser.add_argument('--user', action='store', required=True,
                        help='Modelstatus user name')
    parser.add_argument('--password', action='store', required=True,
                        help='Modelstatus password')
    parser.add_argument('--verify-ssl', dest='verify_ssl', action='store_true', default=True,
                        help='Verify Modelstatus SSL certificate')
    parser.add_argument('--no-verify-ssl', dest='verify_ssl', action='store_false')
    parser.add_argument('--url', action='store', required=True,
                        help='Base URL where local data sets are served')

    args = parser.parse_args()

    setup_logging()

    watcher = ecdiss.recvd.DirectoryWatch(args.input)
    daemon = ecdiss.recvd.daemon.Daemon(
        watcher,
        args.url,
        args.output,
        args.modelstatus,
        args.user,
        args.password,
        args.verify_ssl,
    )

    daemon.main()


if __name__ == '__main__':
    main()
