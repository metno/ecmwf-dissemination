"""
This module contains all threads run by the main program.
"""

import os
import glob
import logging
import logging.config
import argparse
import configparser
import zmq
import threading
import inotify.adapters

import ecreceive
import ecreceive.dataset
import ecreceive.checkpoint


# Listen for kill signals from other threads
ZMQ_KILL_SOCKET = 'tcp://127.0.0.1:9960'
# Pool of threads
ZMQ_WORKERS_SOCKET = 'tcp://127.0.0.1:9970'
# Where to submit jobs to the thread pool
ZMQ_JOB_SUBMIT_SOCKET = 'tcp://127.0.0.1:9980'
# Where to send log file messages
ZMQ_CHECKPOINT_SOCKET = 'tcp://127.0.0.1:9990'
# How many seconds to wait for running threads to complete
THREAD_GRACE = 0


class ZMQThread(threading.Thread):
    """
    Base class for all threads. It provides communication with the main thread,
    enabling the thread to kill the program when an unhandled exception occurs.
    """

    def setup_zmq(self):
        self.context = zmq.Context()
        self.killswitch = self.context.socket(zmq.PUSH)
        self.killswitch.connect(ZMQ_KILL_SOCKET)

    def kill_main_thread(self):
        logging.info("The thread crashed! I'm bringing down the entire application!")
        self.killswitch.send_string('KILL')

    def run(self):
        ecreceive.run_with_exception_logging(self.run_inner)
        self.kill_main_thread()


class CheckpointThread(ZMQThread):
    """
    This thread is responsible for read and write operations from/to the
    checkpoint file. All operations are handled serially, making the file
    operation thread safe.
    """

    def __init__(self, checkpoint_file):
        threading.Thread.__init__(self)
        self.daemon = True
        self.name = 'CheckpointThread'
        self.checkpoint_file = checkpoint_file
        self.checkpoint = ecreceive.checkpoint.Checkpoint(self.checkpoint_file)
        self.setup_zmq()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(ZMQ_CHECKPOINT_SOCKET)

    def run_inner(self):
        logging.info('Checkpoint thread started on %s' % ZMQ_CHECKPOINT_SOCKET)
        self.checkpoint.unlock_all()
        logging.info('All transactions unlocked.')
        while True:
            request = self.socket.recv_json()
            logging.info('Received checkpoint request: %s' % request)
            func = getattr(self.checkpoint, request[0])
            rc = func(*request[1:])
            self.socket.send_json(rc)


class DirectoryWatcherThread(ZMQThread):
    """
    This thread runs inotify on the spool directory, emitting a message each
    time an event is received.
    """

    def __init__(self, spool_directory):
        threading.Thread.__init__(self)
        self.daemon = True
        self.name = 'DirectoryWatcherThread'
        self.setup_zmq()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect(ZMQ_JOB_SUBMIT_SOCKET)
        self.spool_directory = spool_directory
        try:
            self.inotify = inotify.adapters.Inotify()
            self.inotify.add_watch(self.spool_directory.encode('ascii'))
        except:
            raise ecreceive.exceptions.ECReceiveException(
                'Something went wrong when setting up the inotify watch for %s. Does the directory exist, and do you have correct permissions?' % self.spool_directory)

    def process_inotify_event(self, event):
        """
        Perform an action when an inotify event is received.
        """
        if event is None:
            return
        header, attribs, path, filename = event
        if 'IN_CLOSE_WRITE' not in attribs:
            return
        logging.info('Filesystem has IN_CLOSE_WRITE event for %s' % filename)
        if not filename.endswith(b'.md5'):
            logging.info('Ignoring non-md5sum input file.')
            return
        self.socket.send_string(filename.decode('utf-8'))

    def run_inner(self):
        """
        Iterate over inotify file events from the kernel.
        """
        for event in self.inotify.event_gen():
            self.process_inotify_event(event)


class WorkerThread(ZMQThread):
    """
    This thread runs the actual data processing and Productstatus
    communication. The number of started threads is defined in the
    configuration file.
    """

    def __init__(self, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.setup_zmq()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect(ZMQ_WORKERS_SOCKET)

        # Communication with the Checkpoint writer
        self.checkpoint_socket = self.context.socket(zmq.REQ)
        self.checkpoint_socket.connect(ZMQ_CHECKPOINT_SOCKET)

        # This thread might re-submit its own task if it fails due to external factors
        self.resubmit_socket = self.context.socket(zmq.PUSH)
        self.resubmit_socket.connect(ZMQ_JOB_SUBMIT_SOCKET)

        # Dataset processing and publishing
        self.publisher = ecreceive.dataset.DatasetPublisher(
            self.checkpoint_socket,
            kwargs['base_url'],
            kwargs['file_lifetime'],
            kwargs['productstatus_source'],
            kwargs['spool_directory'],
            kwargs['mms_url'],
        )

    def run_inner(self):
        logging.info('Worker thread started')
        while True:
            request = self.socket.recv_string()
            logging.info('Received processing request: %s' % request)
            try:
                self.publisher.process_file(request)
            except ecreceive.exceptions.TryAgainException:
                logging.error('Processing of %s was disrupted; resubmitting to queue' % request)
                self.resubmit_socket.send_string(request)


class DistributionThread(ZMQThread):
    """
    This thread shall load balance job processing requests among a collection of threads.
    """

    def __init__(self, **kwargs):
        threading.Thread.__init__(self)
        self.name = 'DistributionThread'
        self.daemon = True

        self.setup_zmq()
        self.workers = self.context.socket(zmq.PUSH)
        self.workers.bind(ZMQ_WORKERS_SOCKET)
        self.clients = self.context.socket(zmq.PULL)
        self.clients.bind(ZMQ_JOB_SUBMIT_SOCKET)

    def run_inner(self):
        logging.info('Process submission socket listening on %s' % ZMQ_JOB_SUBMIT_SOCKET)
        logging.info('Process distribution socket listening on %s' % ZMQ_WORKERS_SOCKET)
        logging.info('Now distributing processing requests.')
        zmq.proxy(self.clients, self.workers)


class MainThread(object):
    """
    This class is the main program. It reads configuration from a file, starts
    all threads, and kills the threads when a terminate signal is given or a
    thread crashes.
    """

    def __init__(self):
        self.threads = []
        self.context = zmq.Context()
        self.job_submit_socket = self.context.socket(zmq.PUSH)
        self.job_submit_socket.connect(ZMQ_JOB_SUBMIT_SOCKET)
        self.checkpoint_socket = self.context.socket(zmq.REQ)
        self.checkpoint_socket.connect(ZMQ_CHECKPOINT_SOCKET)

    def process_directory(self, directory):
        """
        Process all files in a directory.
        """
        files = list(glob.iglob(os.path.join(directory, '*.md5')))
        logging.info('Processing %d datasets in directory %s.' % (len(files), directory))
        for f in files:
            f = os.path.basename(f)
            logging.info('Sending process request for dataset: %s' % f)
            self.job_submit_socket.send_string(f)
        logging.info('Finished processing %s.' % directory)

    def process_incomplete_checkpoints(self, directories):
        """
        Iterates through files left unprocessed, and does away with them.
        """
        self.checkpoint_socket.send_json(['keys'])
        checkpointed_files = list(self.checkpoint_socket.recv_json())
        n_checkpointed = len(checkpointed_files)
        logging.info('Processing %d incomplete checkpoints.' % n_checkpointed)
        for f in checkpointed_files:
            logging.info('Sending process request for unfinished dataset: %s' % f)
            self.job_submit_socket.send_string(f)
        logging.info('Finished processing incomplete checkpoints.')

    def setup_configuration(self):
        self.argument_parser = argparse.ArgumentParser()
        self.argument_parser.add_argument('--config', action='store', required=True,
                                          help='Path to configuration file')

        # Parse command-line arguments.
        self.args = self.argument_parser.parse_args()

        # Configure logging.
        logging.config.fileConfig(self.args.config)

        logging.info('Starting up ECMWF dissemination receiver.')

        # Read configuration file.
        self.config_parser = configparser.SafeConfigParser()
        self.config_parser.read(self.args.config)

        # Collect parameters for the worker threads.
        self.kwargs = {
            'mms_url': self.config_parser.get('mms', 'url'),
            'spool_directory': self.config_parser.get('ecreceive', 'spool_directory'),
        }

    def main(self):

        # Set up processing threads.
        num_threads = self.config_parser.getint('ecreceive', 'worker_threads')
        for i in range(num_threads):
            thread = WorkerThread(**self.kwargs)
            thread.start()
            self.threads += [thread]

        # Set up the checkpoint writer thread.
        checkpoint_file = self.config_parser.get('ecreceive', 'checkpoint_file')
        checkpoint_thread = CheckpointThread(checkpoint_file)
        checkpoint_thread.start()
        self.threads += [checkpoint_thread]

        # Set up the inotify thread.
        inotify_thread = DirectoryWatcherThread(self.kwargs['spool_directory'])
        inotify_thread.start()
        self.threads += [inotify_thread]

        # Set up the process distribution thread.
        distribution_thread = DistributionThread()
        distribution_thread.start()
        self.threads += [distribution_thread]

        # Listen for kill signals from threads.
        killswitch = self.context.socket(zmq.PULL)
        killswitch.bind(ZMQ_KILL_SOCKET)

        # Run unfinished processing
        self.process_incomplete_checkpoints([
            self.kwargs['spool_directory'],
        ])
        self.process_directory(self.kwargs['spool_directory'])

        # The program is now running until a signal is received on
        # ZMQ_KILL_SOCKET, or an exception is triggered.
        try:
            logging.info('ECMWF dissemination receiver daemon ready.')
            killswitch.recv_string()
        except KeyboardInterrupt:
            pass

    def run(self):

        # Read configuration.
        self.setup_configuration()

        # Catch and log all exceptions
        rc = ecreceive.run_with_exception_logging(self.main)

        # Kill threads
        logging.info('Received shutdown signal, waiting %d seconds for each thread to complete...' % THREAD_GRACE)
        for thread in self.threads:
            logging.info('Killing thread: %s' % thread.name)
            thread.join(THREAD_GRACE)

        logging.info('ECMWF dissemination receiver daemon terminating.')

        return rc
