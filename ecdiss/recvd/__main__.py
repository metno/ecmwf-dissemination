import ecdiss.recvd

import os
import logging
import argparse


def setup_logging():
    logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s', level=logging.DEBUG)


def process_file(fullpath, args):
    """
    Run the recvd business logic on a file:

    1. Check if the file has its data or md5sum counterpart
    2. Validate the data file against the md5sum file
    3. Move the file to its appointed location
    4. TODO: Extract file information
    5. TODO: Post dataset to the Modelstatus service
    """

    # Check if both files exist
    dataset = ecdiss.recvd.Dataset(fullpath)
    if not dataset.complete():
        logging.info('%s: incomplete dataset: %s' % (dataset, dataset.state()))
        return False

    logging.info('%s: ready for processing' % dataset)

    # Check if contents matches md5sum
    try:
        if not dataset.valid():
            logging.warning('%s: md5sum mismatch: data=%s, control=%s' %
                            (dataset, dataset.md5_result, dataset.md5_key))
            return False
    except ecdiss.recvd.InvalidDataException, e:
        logging.warning('%s: validation error: %s' % (dataset, unicode(e)))
        return False

    # Move the files to their proper location
    try:
        dataset.move(args.output)
    except ecdiss.recvd.InvalidDataException, e:
        logging.error('%s: error when moving: %s' % (dataset, unicode(e)))

    logging.info('%s: dataset moved' % dataset)

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', action='store', required=True, help='Directory to watch for input files')
    parser.add_argument('--output', action='store', required=True, help='Move valid input file to this directory')

    args = parser.parse_args()

    setup_logging()

    watcher = ecdiss.recvd.DirectoryWatch(args.input)

    #
    # Main loop.
    # Iterate over inotify file events from the kernel.
    #
    for event in watcher.event_iterator():
        if event is None:
            continue
        header, attribs, path, filename = event
        if 'IN_CLOSE_WRITE' not in attribs:
            continue
        fullpath = os.path.join(path, filename)
        process_file(fullpath, args)


if __name__ == '__main__':
    main()
