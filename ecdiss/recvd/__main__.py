import ecdiss.recvd

import os
import logging
import argparse

def setup_logging():
    logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s', level=logging.DEBUG)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', action='store', required=True, help='Directory to watch for input files')
    parser.add_argument('--output', action='store', required=True, help='Move valid input file to this directory')

    args = parser.parse_args()

    setup_logging()

    watcher = ecdiss.recvd.DirectoryWatch(args.input)

    for event in watcher.event_iterator():
        if event is None:
            continue
        header, attribs, path, filename = event
        if 'IN_CLOSE_WRITE' not in attribs:
            continue
        fullpath = os.path.join(path, filename)
        dataset = ecdiss.recvd.Dataset(fullpath)
        logging.info('Received: %s' % dataset)
        if not dataset.complete():
            continue
        try:
            if not dataset.valid():
                logging.warning('Dataset md5sum mismatch: data=%s, control=%s' %
                        (dataset.md5_result, dataset.md5_key))
                continue
        except ecdiss.recvd.InvalidDataException, e:
            logging.warning('Error validating %s: %s' % (dataset, unicode(e)))
            continue
        dataset.move(args.output)
        logging.info('Dataset has been moved: %s' % dataset)

if __name__ == '__main__':
    main()
