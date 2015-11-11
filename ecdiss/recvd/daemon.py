import os
import logging
import glob
import datetime

import modelstatus.api
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
                 dataset_lifetime,
                 modelstatus_service_backend_uuid,
                 output_path,
                 modelstatus_url,
                 modelstatus_username,
                 modelstatus_api_key,
                 modelstatus_verify_ssl,
                 ):

        self.directory_watch = directory_watch
        self.ecdiss_base_url = ecdiss_base_url
        self.dataset_lifetime = datetime.timedelta(minutes=dataset_lifetime)
        self.output_path = output_path
        self.modelstatus = modelstatus.api.Api(modelstatus_url,
                                               verify_ssl=modelstatus_verify_ssl,
                                               username=modelstatus_username,
                                               api_key=modelstatus_api_key)
        self.modelstatus_service_backend = self.modelstatus.service_backend[modelstatus_service_backend_uuid]
        self.modelstatus_data_formats = {}
        self.modelstatus_models = {}

    def modelstatus_get_or_post(self, collection, data, order_by=None):
        """
        Search for a certain resource type at the Modelstatus server, matching
        the given parameters in the `data` variable. If no records are found,
        one is created using a POST request. Returns a single Resource object.
        """
        # search for existing
        qs = collection.objects
        qs.filter(**data)
        if order_by:
            qs.order_by(order_by)

        # create if not found
        if qs.count() == 0:
            resource = collection.create()
            [setattr(resource, key, value) for key, value in data.iteritems()]
            resource.save()
        else:
            resource = qs[0]

        return resource

    def get_modelstatus_data_format(self, file_type):
        """
        Given a file format string, return a DataFormat object pointing to the
        correct data format.
        """
        if file_type not in self.modelstatus_data_formats:
            qs = self.modelstatus.data_format.objects.filter(name=file_type)
            if qs.count() == 0:
                raise Exception("Data format '%s' was not found on the Modelstatus server" %
                                file_type)
            self.modelstatus_data_formats[file_type] = qs[0]
        return self.modelstatus_data_formats[file_type]

    def get_model_by_grib_metadata(self, data_provider):
        """
        Given a Dataset object, return a matching Model resource at the
        Modelstatus server, or None if no matching model is found.
        """
        if data_provider not in self.modelstatus_models:
            qs = self.modelstatus.model.objects.filter(
                grib_center=data_provider[0],
                grib_generating_process_id=unicode(data_provider[1]),
                )
            if qs.count() == 0:
                raise Exception("Model defined from GRIB center '%s' and generating process id '%d' was not found on the Modelstatus server" % (data_provider[0], data_provider[1]))
            self.modelstatus_models[data_provider] = qs[0]
        return self.modelstatus_models[data_provider]

    def get_or_post_model_run_resource(self, model, reference_time):
        """
        Return a matching ModelRun resource according to Model and reference time.
        """
        parameters = {
            'model': model,
            'reference_time': reference_time,
        }
        order_by = '-version'
        return self.modelstatus_get_or_post(self.modelstatus.model_run, parameters, order_by)

    def get_or_post_model_run_resources(self, dataset):
        """
        Return a list of model run resources at the Modelstatus server,
        matching the provided data set. If any model run resource does not
        exist, it is created.
        """
        resources = []
        for data_provider in dataset.data_providers():
            model = self.get_model_by_grib_metadata(data_provider)
            for reference_time in dataset.reference_times():
                resources += [self.get_or_post_model_run_resource(model, reference_time)]
        return resources

    def get_or_post_data_resource(self, model_run, reference_time):
        """
        Return a matching Data resource according to ModelRun and data file
        begin/end times.
        """
        parameters = {
            'model_run': model_run,
            # FIXME: we should supply time periods
            'time_period_begin': None,
            'time_period_end': None,
        }
        return self.modelstatus_get_or_post(self.modelstatus.data, parameters)

    def post_data_file_resource(self, data, dataset):
        """
        Create a DataFile resource at the Modelstatus server, referring to the
        given data set.
        """
        resource = self.modelstatus.data_file.create()
        resource.data = data
        resource.expires = datetime.datetime.now() + self.dataset_lifetime
        resource.format = self.get_modelstatus_data_format(dataset.file_type())
        resource.service_backend = self.modelstatus_service_backend
        resource.url = self.ecdiss_base_url + dataset.data_filename()
        resource.save()
        return resource

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
            data_resource = self.get_or_post_data_resource(model_run_resource, model_run_resource.reference_time)
            data_file_resource = self.post_data_file_resource(data_resource, dataset)
            logging.info('%s: resource created' % data_file_resource)

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
