import os
import hashlib
import logging
import datetime

import ecreceive.exceptions

import productstatus.exceptions


class Dataset(object):
    """
    The Dataset class represents a combination of a data file and its md5sum
    counterpart file.
    """

    def __init__(self, path):
        """
        Class constructor. Takes the full path to either the md5sum or the data
        file itself, as a string.
        """
        paths = self._derive_paths(path)
        self.data_path = paths["data"]
        self.md5_path = paths["md5"]
        self.md5_key = None
        self.md5_result = None
        self.filename_components = {}

    def _derive_paths(self, path):
        """
        Given a path to either the data file itself or its md5sum counterpart,
        figure out the paths for both of them.
        """
        paths = {}
        if self.is_md5_path(path):
            paths["md5"] = path
            paths["data"] = self.md5_to_data_path(path)
        else:
            paths["md5"] = self.data_to_md5_path(path)
            paths["data"] = path
        return paths

    def is_md5_path(self, path):
        """
        Returns True if the specified path points to an md5sum file, False otherwise.
        """
        return path[-4:] == ".md5"

    def data_to_md5_path(self, path):
        """
        Generate the path containing the MD5 checksum for the specified path.
        """
        return path + ".md5"

    def md5_to_data_path(self, path):
        if not self.is_md5_path(path):
            raise ecreceive.exceptions.ECReceiveException(
                "Cannot derive a data path from a non-md5sum path"
            )
        return path[:-4]

    def has_data_file(self):
        """
        Returns True if the specified md5sum file exists along with a data file, False otherwise.
        """
        return os.path.exists(self.data_path)

    def has_md5_file(self):
        """
        Returns True if the specified data file exists along with an md5sum file, False otherwise.
        """
        return os.path.exists(self.md5_path)

    def complete(self):
        """
        Returns True if both files in the dataset exists, False otherwise.
        """
        return self.has_data_file() and self.has_md5_file()

    def delete(self):
        """!
        @brief Delete both files in the dataset.
        """
        if self.has_data_file():
            logging.info("Deleted data file: '%s'", self.data_path)
            os.unlink(self.data_path)
        else:
            logging.error("Data file does not exist: '%s'", self.data_path)
        if self.has_md5_file():
            logging.info("Deleted md5sum file: '%s'", self.md5_path)
            os.unlink(self.md5_path)
        else:
            logging.error("md5sum file does not exist: '%s'", self.md5_path)

    def read_md5sum(self):
        """
        Read the contents of the md5sum file into memory.
        """
        if not self.has_md5_file():
            raise ecreceive.exceptions.ECReceiveException(
                "Cannot read md5sum without an md5sum file"
            )
        with open(self.md5_path, "r") as f:
            self.md5_key = f.read(32)
            if len(self.md5_key) != 32:
                raise ecreceive.exceptions.InvalidDataException(
                    "md5sum file is less than 32 bytes"
                )

    def calculate_md5sum(self):
        """
        Calculate the md5sum of the data file.
        """
        h = hashlib.md5()
        if not self.has_data_file():
            raise ecreceive.exceptions.ECReceiveException(
                "Cannot calculate md5sum without a data file"
            )
        with open(self.data_path, "r") as f:
            while True:
                data = f.read(256 * 128)  # md5 block size is 128
                if len(data) == 0:
                    break
                h.update(data)
        self.md5_result = h.hexdigest()

    def valid(self):
        """
        Returns True if md5sum matches data file contents.
        """
        if not self.md5_key:
            self.read_md5sum()
        if not self.md5_result:
            self.calculate_md5sum()
        return self.md5_key == self.md5_result

    def md5(self):
        if not self.md5_result:
            self.calculate_md5sum()
        return self.md5_result

    def data_filename(self):
        """
        Return the filename part of the data file path.
        """
        return os.path.basename(self.data_path)

    def move(self, destination):
        """
        Move both files in the dataset to a different directory.
        """
        if not self.complete():
            raise ecreceive.exceptions.ECReceiveException(
                "Dataset must be complete before moving it"
            )
        for member in ["data_path", "md5_path"]:
            path = getattr(self, member)
            destination_path = os.path.join(destination, os.path.basename(path))
            os.rename(path, destination_path)
            setattr(self, member, destination_path)

    def __repr__(self):
        """
        Return a textual representation of this dataset.
        """
        return "<Dataset at %s>" % self.data_path

    def state(self):
        """
        Return a textual representation of the dataset state.
        """
        if self.complete():
            return "complete"
        elif self.has_data_file() and not self.has_md5_file():
            return "missing md5sum"
        elif self.has_md5_file() and not self.has_data_file():
            return "missing data file"
        else:
            return "missing"

    def parse_filename(self, now):
        """
        Return the parsed components of the dataset filename.
        Note that HHMM might be ____.

        A filename looks like this:
        BFS11120600111511001
        ^^                      The stream definition name
          ^                     ECMWF stream use
           ^^^^^^^^             Analysis start time, MMDDHHMM
                   ^^^^^^^^     Analysis end time, MMDDHHMM
                           ^    Dataset version

        For details see:
        http://www.ecmwf.int/en/forecasts/documentation-and-support/data-delivery/manage-transmission-ecpds/real-time-data-file
        """
        if self.filename_components:
            return
        filename = self.data_filename()
        start = filename[3:11]
        end = filename[11:19]
        try:
            self.filename_components[
                "analysis_start_time"
            ] = ecreceive.parse_filename_timestamp(start, now)
            self.filename_components[
                "analysis_end_time"
            ] = ecreceive.parse_filename_timestamp(end, now)
            self.filename_components["name"] = filename[0:2]
            self.filename_components["stream_use"] = filename[2]
            self.filename_components["version"] = int(filename[19:])
        except ValueError:
            self.filename_components = {}
            raise ecreceive.exceptions.InvalidFilenameException(
                "Filename %s does not match expected format" % self.data_filename()
            )

    def analysis_start_time(self):
        """
        Return the analysis start time of this dataset, according to the filename.
        """
        self.parse_filename(datetime.datetime.now())
        return self.filename_components["analysis_start_time"]

    def analysis_end_time(self):
        """
        Return the analysis end time of this dataset, according to the filename.
        """
        self.parse_filename(datetime.datetime.now())
        return self.filename_components["analysis_end_time"]

    def name(self):
        """
        Return the dataset name, according to the filename.
        """
        self.parse_filename(datetime.datetime.now())
        return self.filename_components["name"]

    def stream_use(self):
        """
        Return the dataset name, according to the filename.
        """
        self.parse_filename(datetime.datetime.now())
        return self.filename_components["stream_use"]

    def version(self):
        """
        Return the dataset version, according to the filename.
        """
        self.parse_filename(datetime.datetime.now())
        return self.filename_components["version"]

    def file_type(self):
        """
        Return the file type of this dataset.
        """
        return "GRIB"


class DatasetPublisher(object):
    """
    Publish datasets from an 'incoming' directory to a destination directory,
    and publish them on a Productstatus server.

    When a file is processed, the class checks whether the file is a) weather
    model data or b) its corresponding md5sum file. If both files exists, the
    pair is submitted for the following processing:

        1. Validate the data file against the md5sum file
        2. Move the file to its appointed location
        3. Extract file information
        4. Post dataset to the Productstatus service

    After processing is complete, the class forgets about the files.
    """

    def __init__(
        self,
        checkpoint_socket,
        ecreceive_base_url,
        dataset_lifetime,
        productstatus_service_backend_uuid,
        productstatus_source_uuid,
        spool_path,
        destination_path,
        productstatus_api,
    ):

        self.ecreceive_base_url = ecreceive_base_url
        self.dataset_lifetime = datetime.timedelta(minutes=dataset_lifetime)
        self.spool_path = spool_path
        self.destination_path = destination_path
        self.checkpoint_socket = checkpoint_socket
        self.productstatus = productstatus_api
        self.productstatus_service_backend_uuid = productstatus_service_backend_uuid
        self.productstatus_source_uuid = productstatus_source_uuid

    def get_dataset_key(self, dataset):
        return dataset.data_filename()

    def checkpoint_zeromq_rpc(self, *args):
        """
        Remote procedure call to the checkpoint thread.
        """
        self.checkpoint_socket.send_json(args)
        return self.checkpoint_socket.recv_json()

    def checkpoint_add(self, dataset, flag):
        return self.checkpoint_zeromq_rpc("add", self.get_dataset_key(dataset), flag)

    def checkpoint_get(self, dataset):
        return self.checkpoint_zeromq_rpc("get", self.get_dataset_key(dataset))

    def checkpoint_delete(self, dataset):
        return self.checkpoint_zeromq_rpc("delete", self.get_dataset_key(dataset))

    def checkpoint_lock(self, dataset):
        return self.checkpoint_zeromq_rpc("lock", self.get_dataset_key(dataset))

    def checkpoint_unlock(self, dataset):
        return self.checkpoint_zeromq_rpc("unlock", self.get_dataset_key(dataset))

    def get_productstatus_dataformat(self, dataset):
        """
        Given a Dataset object, return a DataFormat object pointing to the
        correct data format.
        """
        file_type = dataset.file_type()
        qs = self.productstatus.dataformat.objects.filter(name=file_type)
        if qs.count() == 0:
            raise ecreceive.exceptions.ECReceiveProductstatusException(
                "Data format '%s' was not found on the Productstatus server" % file_type
            )
        resource = qs[0]
        logging.info("%s: Productstatus dataformat for %s" % (resource, file_type))
        return resource

    def get_productstatus_product(self, dataset):
        """
        Given a Dataset object, return a matching Product resource at the
        Productstatus server, or None if no matching product is found.
        """
        name = dataset.name()
        qs = self.productstatus.product.objects.filter(
            source_key=name,
            source=self.productstatus.institution[self.productstatus_source_uuid],
        )
        name_desc = "ECMWF stream name '%s'" % name
        if qs.count() == 0:
            raise ecreceive.exceptions.ECReceiveProductstatusException(
                "Product defined from %s was not found on the Productstatus server"
                % name_desc
            )
        resource = qs[0]
        logging.info("%s: Productstatus Product for %s" % (resource, name_desc))
        return resource

    def get_or_post_productinstance_resource(self, dataset):
        """
        Return a matching ProductInstance resource according to Product, reference time and version.
        """
        product = self.get_productstatus_product(dataset)
        parameters = {
            "product": product,
            "reference_time": dataset.analysis_start_time(),
            "version": dataset.version(),
        }
        return self.productstatus.productinstance.find_or_create(parameters)

    def get_or_post_data_resource(self, productinstance, dataset):
        """
        Return a matching Data resource according to ProductInstance and data file
        begin/end times.
        We are using time_period_begin = time_period_end = The forecast for hour time_period_end
        """
        parameters = {
            "productinstance": productinstance,
            "time_period_begin": dataset.analysis_end_time(),
            "time_period_end": dataset.analysis_end_time(),
        }
        return self.productstatus.data.find_or_create(parameters)

    def post_datainstance_resource(self, data, dataset):
        """
        Create a DataInstance resource at the Productstatus server, referring to the
        given data set.
        """
        resource = self.productstatus.datainstance.create()
        resource.data = data
        resource.expires = datetime.datetime.now() + self.dataset_lifetime
        resource.format = self.get_productstatus_dataformat(dataset)
        resource.servicebackend = self.productstatus.servicebackend[
            self.productstatus_service_backend_uuid
        ]
        resource.url = self.ecreceive_base_url + dataset.data_filename()
        resource.hash = dataset.md5()
        resource.hash_type = "md5"
        resource.save()
        return resource

    def process_file(self, filename):
        """
        Run the recvd business logic on a file; see class description.
        Returns True if the dataset was completely processed, False otherwise.
        """

        logging.info("===== %s: start processing =====" % filename)

        # Check for the file in both spool and destination directories
        for directory in [self.destination_path, self.spool_path]:
            full_path = os.path.join(directory, filename)
            logging.info("Checking if %s exists..." % full_path)
            if os.path.exists(full_path):
                break
            full_path = None

        if not full_path:
            logging.info(
                "Files are not present in any directory. Possible race condition; ignoring."
            )
            return False

        # Instantiate Dataset object
        dataset = ecreceive.dataset.Dataset(full_path)
        logging.info(unicode(dataset))

        # Check if both files exist
        if not dataset.complete():
            logging.info("Incomplete dataset: %s." % dataset.state())
            return False

        # Try to get a lock on this dataset
        if not self.checkpoint_lock(dataset):
            logging.warning("Unable to get a lock on dataset, conflicting thread?")
            return False

        # Register a checkpoint for this dataset to indicate that it exists
        self.checkpoint_add(dataset, ecreceive.checkpoint.CHECKPOINT_DATASET_EXISTS)

        # Check if contents matches md5sum
        try:
            logging.info(
                "%s: now calculating md5sum, this might take a while..." % dataset
            )
            if not dataset.valid():
                logging.error(
                    "md5sum mismatch: data=%s, control=%s."
                    % (dataset.md5_result, dataset.md5_key)
                )
                self.checkpoint_unlock(dataset)
                return False
            logging.info(
                "Calculated md5sum matches reference file: %s." % dataset.md5_result
            )
        except ecreceive.exceptions.InvalidDataException, e:
            logging.error("md5sum validation error: %s." % unicode(e))
            self.checkpoint_unlock(dataset)
            return False

        # Move the files to their proper location
        if not (
            self.checkpoint_get(dataset) & ecreceive.checkpoint.CHECKPOINT_DATASET_MOVED
        ):
            try:
                dataset.move(self.destination_path)
            except ecreceive.exceptions.ECReceiveException, e:
                logging.error("Error when moving: %s." % unicode(e))
                raise ecreceive.exceptions.TryAgainException("Dataset cannot be moved")

            logging.info("Dataset moved and is now known as: %s." % dataset)

            # Record the move in the checkpoint
            self.checkpoint_add(dataset, ecreceive.checkpoint.CHECKPOINT_DATASET_MOVED)
        else:
            logging.info("Skipping move; already moved according to checkpoint.")

        # Obtain Productstatus IDs for this product instance, and submit data files
        def productstatus_submit():

            # Get or create a ProductInstance remote resource
            logging.info("Determining which ProductInstance to post to...")
            productinstance_resource = self.get_or_post_productinstance_resource(
                dataset
            )
            logging.info(
                "Now posting to ProductInstance: %s." % productinstance_resource
            )

            # Get or create a Data remote resource
            logging.info("Determining which Data resource to post to...")
            data_resource = self.get_or_post_data_resource(
                productinstance_resource, dataset
            )

            # Create a DataInstance remote resource
            logging.info("Creating a DataInstance resource...")
            datainstance_resource = self.post_datainstance_resource(
                data_resource, dataset
            )
            logging.info("%s: DataInstance resource created." % datainstance_resource)

            # Everything has been saved at the remote server
            logging.info(
                "Now publicly available at %s until %s."
                % (
                    datainstance_resource.url,
                    datainstance_resource.expires.strftime("%Y-%m-%dT%H:%M:%S%z"),
                )
            )

            # All done
            self.checkpoint_delete(dataset)
            logging.info("===== %s: all done; processed successfully. =====" % dataset)

            return True

        # Run the above function indefinitely
        rc = ecreceive.retry_n(
            productstatus_submit,
            exceptions=(
                productstatus.exceptions.ServiceUnavailableException,
                ecreceive.exceptions.ECReceiveProductstatusException,
            ),
        )
        if rc:
            return

        self.checkpoint_unlock(dataset)
        raise ecreceive.exceptions.TryAgainException(
            "Processing disrupted due to external dependency failure"
        )
