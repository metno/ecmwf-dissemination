import os
import logging
import glob
import json
import time
import datetime

import productstatus.exceptions

import ecdiss.recvd
import ecdiss.recvd.daemon


CHECKPOINT_DATASET_NOFLAGS = 0
CHECKPOINT_DATASET_EXISTS = 1
CHECKPOINT_DATASET_MOVED = 2


def retry_n(func, interval=5, exceptions=(Exception,), warning=1, error=3, give_up=5):
    """
    Call 'func' and, if it throws anything listed in 'exceptions', catch it and retry again
    up to 'give_up' times.
    Assumes that give_up > error > warning > 0.
    """
    tries = 0
    while True:
        try:
            return func()
        except exceptions, e:
            tries += 1
            if tries >= give_up:
                logging.error('Action failed %d times, giving up: %s' % (give_up, e))
                return
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
        try:
            with open(self._path, 'w') as f:
                f.write(data)
        except IOError:
            logging.error('State file %s cannot be written' % self._path)
            raise

    def load(self):
        self._states = {}
        try:
            with open(self._path, 'r') as f:
                data = f.read()
            if data:
                self._states = json.loads(data)
        except IOError:
            logging.info('State file %s does not exist, starting from scratch' % self._path)

    def keys(self):
        return self._states.keys()

    def get(self, key):
        if key not in self._states:
            return 0
        return self._states[key]

    def add(self, key, state):
        if key not in self._states:
            self._states[key] = CHECKPOINT_DATASET_NOFLAGS
        self._states[key] |= state
        self.save()

    def delete(self, key):
        if key in self._states:
            del self._states[key]
            self.save()


def productstatus_get_or_post(collection, data, order_by=None):
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


class DatasetPublisher(object):
    """
    Publish datasets from an 'incoming' directory on a productstatus server.

    Notification about files, either via 'process_file' or 'process_incomplete_checkpoints',
    triggers a check whether the file is a) weather model data or b)
    its corresponding md5sum file. If both files exists, the pair is submitted for
    the following processing:

        1. Validate the data file against the md5sum file
        2. Move the file to its appointed location
        3. Extract file information
        4. Post dataset to the Productstatus service

    After processing is complete, this objects forgets about the data set / file.
    """

    def __init__(self,
                 checkpoint,
                 ecdiss_base_url,
                 dataset_lifetime,
                 productstatus_service_backend,
                 output_path,
                 productstatus_api,
                 ):

        self.ecdiss_base_url = ecdiss_base_url
        self.dataset_lifetime = datetime.timedelta(minutes=dataset_lifetime)
        self.output_path = output_path
        self.checkpoint = checkpoint
        self.productstatus = productstatus_api
        self.productstatus_service_backend = productstatus_service_backend
        self.productstatus_dataformats = {}
        self.productstatus_products = {}

    def get_dataset_key(self, dataset):
        return dataset.data_filename()

    def checkpoint_add(self, dataset, flag):
        self.checkpoint.add(self.get_dataset_key(dataset), flag)

    def checkpoint_get(self, dataset):
        return self.checkpoint.get(self.get_dataset_key(dataset))

    def checkpoint_delete(self, dataset):
        self.checkpoint.delete(self.get_dataset_key(dataset))

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
        return productstatus_get_or_post(self.productstatus.productinstance, parameters, order_by)

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
        return productstatus_get_or_post(self.productstatus.data, parameters)

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
        self.checkpoint_add(dataset, CHECKPOINT_DATASET_EXISTS)

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
        if not self.checkpoint_get(dataset) & CHECKPOINT_DATASET_MOVED:
            try:
                dataset.move(self.output_path)
            except ecdiss.recvd.InvalidDataException, e:
                logging.error('Error when moving: %s.' % unicode(e))

            logging.info('Dataset moved and is now known as: %s.' % dataset)
        else:
            logging.info('Skipping move; already moved according to checkpoint.')

        # Record the move in the checkpoint
        self.checkpoint_add(dataset, CHECKPOINT_DATASET_MOVED)

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

            # All done
            self.checkpoint_delete(dataset)
            logging.info('===== %s: all done; processed successfully. =====' % dataset)

            return True

        # Run the above function indefinitely
        retry_n(
            productstatus_submit,
            exceptions=(
                productstatus.exceptions.ServiceUnavailableException,
                ecdiss.recvd.EcdissProductstatusException
            )
        )

        return False

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
        n_checkpointed = len(checkpointed_files)
        if n_checkpointed == 0:
            return
        logging.info('Processing %d incomplete checkpoints.' % n_checkpointed)
        for f in checkpointed_files:
            for d in directories:
                path = os.path.join(d, f)
                self.process_file(path)
        logging.info('Finished processing incomplete checkpoints.')


class DirectoryDaemon(object):
    """
    Watch a directory for new files
    """

    def __init__(self, directory, callback_new_file=None, callback_timeout=None, timeout_s=1):
        self.callback_new_file = callback_new_file
        self.callback_timeout = callback_timeout
        self.directory_watch = ecdiss.recvd.DirectoryWatch(directory, timeout_s)

    def process_inotify_event(self, event):
        """
        Perform an action when an inotify event is received.
        """
        if (event is None) and self.callback_timeout:
            self.callback_timeout()
        elif self.callback_new_file:
            header, attribs, path, filename = event
            if 'IN_CLOSE_WRITE' not in attribs:
                return
            full_path = os.path.join(path, filename)
            self.callback_new_file(full_path)

    def run(self):
        """
        Main loop.
        Iterates over inotify file events from the kernel.
        """
        for event in self.directory_watch.event_iterator():
            self.process_inotify_event(event)
