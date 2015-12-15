import os
import logging
import glob
import json
import time
import datetime

import productstatus.api
import productstatus.exceptions

import ecdiss.recvd
import ecdiss.recvd.daemon


CHECKPOINT_DATASET_NOFLAGS = 0
CHECKPOINT_DATASET_EXISTS = 1
CHECKPOINT_DATASET_MOVED = 2


def retry_indefinitely(func, interval=5, exceptions=(Exception,), warning=1, error=3):
    """
    Retry a command indefinitely until it succeeds, catching all exceptions
    specified in the function parameter.
    """
    tries = 0
    while True:
        try:
            return func()
        except exceptions, e:
            tries += 1
            if tries >= error:
                logfunc = logging.error
            elif tries >= warning:
                logfunc = logging.warning
            else:
                logfunc = logging.info
            logfunc('Action failed, retrying in %d seconds: %s' % (interval, e))
            time.sleep(interval)


class Checkpoint(object):
    """
    This class creates a state file, which keeps a key/value store of Datasets
    and their states.
    """
    def __init__(self, path):
        self._states = {}
        self._path = path
        self.load()

    def save(self):
        data = json.dumps(self._states, sort_keys=True, indent=4, separators=(',', ': '))
        with open(self._path, 'w') as f:
            f.write(data)

    def load(self):
        try:
            with open(self._path, 'r') as f:
                data = f.read()
            self._states = json.loads(data)
        except IOError:
            logging.info('State file %s does not exist, starting from scratch' % self._path)
            self._states = {}

    def keys(self):
        return self._states.keys()

    def get_dataset_key(self, dataset):
        return dataset.data_filename()

    def get(self, dataset):
        key = self.get_dataset_key(dataset)
        if key not in self._states:
            return 0
        return self._states[key]

    def add(self, dataset, state):
        key = self.get_dataset_key(dataset)
        if key not in self._states:
            self._states[key] = CHECKPOINT_DATASET_NOFLAGS
        self._states[key] |= state
        self.save()

    def delete(self, dataset):
        key = self.get_dataset_key(dataset)
        if key in self._states:
            del self._states[key]
            self.save()


class Daemon(object):
    """
    ECMWF dissemination daemon

    This class monitors a directory for written files. Once a file has been
    written, the daemon checks whether the file is a) weather model data or b)
    its corresponding md5sum file. If both files exists, the pair is submitted for
    the following processing:

        1. Validate the data file against the md5sum file
        2. Move the file to its appointed location
        3. Extract file information
        4. Post dataset to the Productstatus service

    After processing is complete, the daemon forgets about the data set.
    """

    def __init__(self,
                 directory_watch,
                 checkpoint,
                 ecdiss_base_url,
                 dataset_lifetime,
                 productstatus_service_backend_uuid,
                 output_path,
                 productstatus_url,
                 productstatus_username,
                 productstatus_api_key,
                 productstatus_verify_ssl,
                 ):

        self.directory_watch = directory_watch
        self.ecdiss_base_url = ecdiss_base_url
        self.dataset_lifetime = datetime.timedelta(minutes=dataset_lifetime)
        self.output_path = output_path
        self.checkpoint = checkpoint
        self.productstatus = productstatus.api.Api(productstatus_url,
                                                   verify_ssl=productstatus_verify_ssl,
                                                   username=productstatus_username,
                                                   api_key=productstatus_api_key)
        self.productstatus_service_backend = self.productstatus.servicebackend[productstatus_service_backend_uuid]
        self.productstatus_dataformats = {}
        self.productstatus_products = {}

    def productstatus_get_or_post(self, collection, data, order_by=None):
        """
        Search for a certain resource type at the Productstatus server, matching
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
            logging.info('No matching %s resource found, creating...' % collection._resource_name)
            resource = collection.create()
            [setattr(resource, key, value) for key, value in data.iteritems()]
            resource.save()
            logging.info('%s: resource created' % resource)
        else:
            resource = qs[0]
            logging.info('%s: using existing resource' % resource)

        return resource

    def get_productstatus_dataformat(self, dataset):
        """
        Given a Dataset object, return a DataFormat object pointing to the
        correct data format.
        """
        file_type = dataset.file_type()
        if file_type not in self.productstatus_dataformats:
            qs = self.productstatus.dataformat.objects.filter(name=file_type)
            if qs.count() == 0:
                raise ecdiss.recvd.EcdissProductstatusException(
                    "Data format '%s' was not found on the Productstatus server" % file_type
                )
            resource = qs[0]
            self.productstatus_dataformats[file_type] = resource
            logging.info('%s: Productstatus dataformat for %s' % (resource, file_type))
        return self.productstatus_dataformats[file_type]

    def get_productstatus_product(self, dataset):
        """
        Given a Dataset object, return a matching Product resource at the
        Productstatus server, or None if no matching product is found.
        """
        name = dataset.name()
        if name not in self.productstatus_products:
            qs = self.productstatus.product.objects.filter(foreign_id=name, foreign_id_type='ecmwf')
            name_desc = "ECMWF stream name '%s'" % name
            if qs.count() == 0:
                raise ecdiss.recvd.EcdissProductstatusException(
                    "Product defined from %s was not found on the Productstatus server" % name_desc
                )
            resource = qs[0]
            self.productstatus_products[name] = resource
            logging.info("%s: Productstatus Product for %s" % (resource, name_desc))
        return self.productstatus_products[name]

    def get_or_post_productinstance_resource(self, dataset):
        """
        Return a matching ProductRun resource according to Product and reference time.
        """
        product = self.get_productstatus_product(dataset)
        parameters = {
            'product': product,
            'reference_time': dataset.analysis_start_time(),
            # FIXME: add version to POST, see T2084
        }
        order_by = '-version'
        return self.productstatus_get_or_post(self.productstatus.productinstance, parameters, order_by)

    def get_or_post_data_resource(self, productinstance, dataset):
        """
        Return a matching Data resource according to ProductRun and data file
        begin/end times.
        """
        parameters = {
            'productinstance': productinstance,
            'time_period_begin': dataset.analysis_start_time(),
            'time_period_end': dataset.analysis_end_time(),
        }
        return self.productstatus_get_or_post(self.productstatus.data, parameters)

    def post_datainstance_resource(self, data, dataset):
        """
        Create a DataInstance resource at the Productstatus server, referring to the
        given data set.
        """
        resource = self.productstatus.datainstance.create()
        resource.data = data
        resource.expires = datetime.datetime.now() + self.dataset_lifetime
        resource.format = self.get_productstatus_dataformat(dataset)
        resource.servicebackend = self.productstatus_service_backend
        resource.url = self.ecdiss_base_url + dataset.data_filename()
        resource.save()
        return resource

    def process_file(self, full_path):
        """
        Run the recvd business logic on a file; see class description.
        Returns True if the dataset was completely processed, False otherwise.
        """

        dataset = ecdiss.recvd.Dataset(full_path)
        logging.info('===== %s: start processing =====' % dataset)

        # Check if both files exist
        if not dataset.complete():
            logging.info('Incomplete dataset: %s.' % dataset.state())
            return False

        # Register a checkpoint for this dataset to indicate that it exists
        self.checkpoint.add(dataset, CHECKPOINT_DATASET_EXISTS)

        # Check if contents matches md5sum
        try:
            if not dataset.valid():
                logging.warning('md5sum mismatch: data=%s, control=%s.' %
                                (dataset.md5_result, dataset.md5_key))
                return False
            logging.info('Calculated md5sum matches reference file: %s.' % dataset.md5_result)
        except ecdiss.recvd.InvalidDataException, e:
            logging.warning('Validation error: %s.' % unicode(e))
            return False

        # Move the files to their proper location
        if not self.checkpoint.get(dataset) & CHECKPOINT_DATASET_MOVED:
            try:
                dataset.move(self.output_path)
            except ecdiss.recvd.InvalidDataException, e:
                logging.error('Error when moving: %s.' % unicode(e))

            logging.info('Dataset moved and is now known as: %s.' % dataset)
        else:
            logging.info('Skipping move; already moved according to checkpoint.')

        # Record the move in the checkpoint
        self.checkpoint.add(dataset, CHECKPOINT_DATASET_MOVED)

        # Obtain Productstatus IDs for this product instance, and submit data files
        def productstatus_submit():

            # Get or create a ProductInstance remote resource
            logging.info('Determining which ProductInstance to post to...')
            productinstance_resource = self.get_or_post_productinstance_resource(dataset)
            logging.info('Now posting to ProductInstance: %s.' % productinstance_resource)

            # Get or create a Data remote resource
            logging.info('Determining which Data resource to post to...')
            data_resource = self.get_or_post_data_resource(productinstance_resource, dataset)

            # Create a DataInstance remote resource
            logging.info('Creating a DataInstance resource...')
            datainstance_resource = self.post_datainstance_resource(data_resource, dataset)
            logging.info("%s: DataInstance resource created." % datainstance_resource)

            # Everything has been saved at the remote server
            logging.info("Now publicly available at %s until %s." % (
                datainstance_resource.url,
                datainstance_resource.expires.strftime('%Y-%m-%dT%H:%M:%S%z'),
            ))

        # Run the above function indefinitely
        retry_indefinitely(
            productstatus_submit,
            exceptions=(
                productstatus.exceptions.ServiceUnavailableException,
                ecdiss.recvd.EcdissProductstatusException
            )
        )

        # All done
        self.checkpoint.delete(dataset)
        logging.info('===== %s: all done; processed successfully. =====' % dataset)

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
        logging.info('%s: processing directory.' % directory)
        files = list(glob.iglob(os.path.join(directory, '*.md5')))
        logging.info('Directory contains %d files.' % len(files))
        for f in files:
            self.process_file(f)
        logging.info('%s: finished processing directory.' % directory)

    def process_incomplete_checkpoints(self, directories):
        """
        Iterates through files left unprocessed, and does away with them.
        """
        checkpointed_files = list(self.checkpoint.keys())
        logging.info('Processing %d incomplete checkpoints.' % len(checkpointed_files))
        for f in checkpointed_files:
            for d in directories:
                path = os.path.join(d, f)
                self.process_file(path)
        logging.info('Finished processing incomplete checkpoints.')

    def main(self):
        """
        Main loop.
        Iterates over inotify file events from the kernel.
        """
        for event in self.directory_watch.event_iterator():
            self.process_inotify_event(event)
