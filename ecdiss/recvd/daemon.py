import os
import logging
import glob

import modelstatus
import modelstatus.exceptions

import ecdiss.recvd
import ecdiss.recvd.daemon


class Daemon(object):
    """
    ECMWF dissemination daemon

    This class monitors a directory for written files. Once a file has been
    written, the daemon checks whether the file is weather model data or its
    corresponding md5sum file. If both files exists, the pair is submitted for
    the following processing:

        1. Validate the data file against the md5sum file
        2. Move the file to its appointed location
        3. Extract file information
        4. Post dataset to the Modelstatus service

    After processing is complete, the daemon forgets about the data set.
    """

    def __init__(self,
                 directory_watch,
                 ecdiss_base_url,
                 output_path,
                 modelstatus_url,
                 modelstatus_username,
                 modelstatus_password,
                 modelstatus_verify_ssl):

        self.directory_watch = directory_watch
        self.ecdiss_base_url = ecdiss_base_url
        self.output_path = output_path
        self.modelstatus_url = modelstatus_url
        self.modelstatus_username = modelstatus_username
        self.modelstatus_password = modelstatus_password
        self.modelstatus_verify_ssl = modelstatus_verify_ssl

        args = [self.modelstatus_url]
        kwargs = {
            'verify_ssl': self.modelstatus_verify_ssl,
            'username': self.modelstatus_username,
            'password': self.modelstatus_password
        }

        self.model_run_collection = modelstatus.ModelRunCollection(*args, **kwargs)
        self.data_collection = modelstatus.DataCollection(*args, **kwargs)

    def get_or_post_model_run_resource(self, data_provider, reference_time):
        """
        Search for a model_run resource at the Modelstatus server, matching the
        given data provider and reference time. If no records are found, one is
        created using a POST request.
        """
        parameters = {
            'data_provider': data_provider,
            'reference_time': reference_time.strftime('%Y-%m-%dT%H:%M:%S%Z'),
        }

        # Execute a remote search for a model run
        resources = self.model_run_collection.filter(**parameters)

        # Create a new model run if none was found
        if len(resources) == 0:
            resource = self.model_run_collection.post(parameters)
        else:
            resource = resources[0]

        return resource

    def get_or_post_model_run_resources(self, dataset):
        """
        Return a list of model run resources at the Modelstatus server,
        matching the provided data set. If any model run resource does not
        exist, it is created.
        """
        resources = []
        for data_provider in dataset.data_providers():
            for reference_time in dataset.reference_times():
                resources += [self.get_or_post_model_run_resource(data_provider, reference_time)]
        return resources

    def post_data_resource(self, dataset, model_run_resource):
        """
        Create a data resource at the Modelstatus server, referring to the
        given data set.
        """
        parameters = {
            'model_run_id': model_run_resource.id,
            'format': dataset.file_type(),
            'href': self.ecdiss_base_url + dataset.data_filename()
        }
        return self.data_collection.post(parameters)

    def process_file(self, full_path):
        """
        Run the recvd business logic on a file; see class description.
        Returns True if the dataset was completely processed, False otherwise.
        """
        # Check if both files exist
        dataset = ecdiss.recvd.Dataset(full_path)
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
            dataset.move(self.output_path)
        except ecdiss.recvd.InvalidDataException, e:
            logging.error('%s: error when moving: %s' % (dataset, unicode(e)))

        logging.info('%s: dataset moved' % dataset)

        # Obtain Modelstatus IDs for this model run, and submit data files
        model_run_resources = self.get_or_post_model_run_resources(dataset)
        for model_run_resource in model_run_resources:
            data_resource = self.post_data_resource(dataset, model_run_resource)
            logging.info('%s: resource created' % model_run_resource)
            logging.info('%s: resource created' % data_resource)

        # All done
        logging.info('%s: all done; processed successfully' % dataset)

        return True

    def process_inotify_event(self, event):
        """
        Perform an action when an inotify event is received.
        """
        if event is None:
            return
        header, attribs, path, filename = event
        if 'IN_CLOSE_WRITE' not in attribs:
            return
        full_path = os.path.join(path, filename)
        return self.process_file(full_path)

    def process_directory(self, directory):
        """
        Process all files in a directory.
        """
        logging.info('%s: processing directory' % directory)
        files = glob.iglob(os.path.join(directory, '*.md5'))
        for f in files:
            self.process_file(f)
        logging.info('%s: finished processing directory' % directory)

    def main(self):
        """
        Main loop.
        Iterates over inotify file events from the kernel.
        """
        for event in self.directory_watch.event_iterator():
            self.process_inotify_event(event)
