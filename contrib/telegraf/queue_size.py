#!/usr/bin/env python

import os
import argparse
import json

def get_files_size(files):
    """!
    @brief Return the sum of the size of all provided files.
    """
    return sum(os.path.getsize(f) for f in files)

def get_files_count(files, suffix):
    """!
    @brief Return the file count of files having a specific file suffix.
    """
    return len([f for f in files if os.path.splitext(f)[1] == suffix])

def get_all_metrics(path):
    """!
    @brief Return a dictionary with all metrics for a specified path.
    """
    files = [os.path.join(path, f) for f in os.listdir(path)]
    files = [f for f in files if os.path.isfile(f)]
    return {
        'count': {
            'data': get_files_count(files, ''),
            'tmp': get_files_count(files, 'tmp'),
            'md5': get_files_count(files, 'md5'),
        },
        'size': get_files_size(files),
    }

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script checks the ECMWF incoming and destination directories, and prints the metrics in JSON format. Convenient for use with Telegraf.')
    parser.add_argument('queue', help='Path to ECMWF incoming directory')
    parser.add_argument('destination', help='Path to destination directory')
    args = parser.parse_args()
    data = {
        'queue': get_all_metrics(args.queue),
        'destination': get_all_metrics(args.destination),
    }
    print json.dumps(data, indent=4)
