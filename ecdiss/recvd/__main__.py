import ecdiss.recvd

import logging
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', action='store', required=True, help='Directory to watch for input files')

    args = parser.parse_args()

    watcher = ecdiss.recvd.DirectoryWatch(args.directory)

    for event in watcher.event_iterator():
        if event is None:
            continue
        header, attribs, path, filename = event
        if 'IN_CLOSE_WRITE' not in attribs:
            continue
        print event

if __name__ == '__main__':
    main()
