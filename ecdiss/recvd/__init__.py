import os
import hashlib
import dateutil.tz
import inotify.adapters
import pygrib

READ_BUFFER = 8192


class EcdissException(Exception):
    pass


class InvalidDataException(EcdissException):
    pass


class Dataset(object):
    """
    The Dataset class represents a combination of a data file and its md5sum
    counterpart file.
    """
    def __init__(self, path):
        paths = self._derive_paths(path)
        self.data_path = paths['data']
        self.md5_path = paths['md5']
        self.md5_key = None
        self.md5_result = None
        self.grib_reader = None
        self.grib_data = None

    def _derive_paths(self, path):
        """
        Given a path to either the data file itself or its md5sum counterpart,
        figure out the paths for both of them.
        """
        paths = {}
        if self.is_md5_path(path):
            paths['md5'] = path
            paths['data'] = self.md5_to_data_path(path)
        else:
            paths['md5'] = self.data_to_md5_path(path)
            paths['data'] = path
        return paths

    def is_md5_path(self, path):
        """
        Returns True if the specified path points to an md5sum file, False otherwise.
        """
        return path[-4:] == '.md5'

    def data_to_md5_path(self, path):
        """
        Generate the path containing the MD5 checksum for the specified path.
        """
        return path + '.md5'

    def md5_to_data_path(self, path):
        if not self.is_md5_path(path):
            raise EcdissException('Cannot derive a data path from a non-md5sum path')
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

    def read_md5sum(self):
        """
        Read the contents of the md5sum file into memory.
        """
        if not self.has_md5_file():
            raise EcdissException('Cannot read md5sum without an md5sum file')
        with open(self.md5_path, 'r') as f:
            self.md5_key = f.read(32)
            if len(self.md5_key) != 32:
                raise InvalidDataException('md5sum file is less than 32 bytes')

    def calculate_md5sum(self):
        """
        Calculate the md5sum of the data file.
        """
        h = hashlib.md5()
        if not self.has_data_file():
            raise EcdissException('Cannot calculate md5sum without a data file')
        with open(self.data_path, 'r') as f:
            while True:
                data = f.read(READ_BUFFER)
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

    def open_grib(self):
        """
        Open the data file as a GRIB object.
        """
        if self.grib_reader:
            return
        if not self.has_data_file():
            raise EcdissException('Cannot get GRIB file handle: missing data file at %s' % self.data_path)
        self.grib_reader = pygrib.open(self.data_path)
        self.grib_data = self.grib_reader.read()
        if len(self.grib_data) == 0:
            raise EcdissException('GRIB data set is empty')

    def data_filename(self):
        """
        Return the filename part of the data file path.
        """
        return os.path.basename(self.data_path)

    def move(self, destination):
        """
        Move the dataset to a different directory.
        """
        if not self.complete():
            raise EcdissException('Dataset must be complete before moving it')
        for member in ['data_path', 'md5_path']:
            path = getattr(self, member)
            destination_path = os.path.join(destination, os.path.basename(path))
            os.rename(path, destination_path)
            setattr(self, member, destination_path)

    def __repr__(self):
        """
        Return a textual representation of this dataset.
        """
        return 'Dataset at %s' % self.data_path

    def state(self):
        """
        Return a textual representation of the dataset state.
        """
        if self.complete():
            return 'complete'
        elif self.has_data_file() and not self.has_md5_file():
            return 'missing md5sum'
        elif self.has_md5_file() and not self.has_data_file():
            return 'missing data file'
        else:
            return 'missing'

    def force_utc(self, timestamp):
        """
        Force a "naive" timestamp into UTC, or return the original timestamp
        for sane timestamps.
        """
        if not timestamp.tzinfo:
            return timestamp.replace(tzinfo=dateutil.tz.tzutc())
        return timestamp

    def reference_times(self):
        """
        Return a list of model reference timestamps for this dataset.
        """
        self.open_grib()
        return list(set(sorted([self.force_utc(grib.analDate) for grib in self.grib_data])))

    def data_providers(self):
        """
        Return the data provider names of this dataset. Data provider names
        consists of a combination of generating center and generating process
        identifier.
        """
        self.open_grib()
        providers = [(grib.centre, grib.generatingProcessIdentifier) for grib in self.grib_data]
        return list(set(sorted(providers)))

    def file_type(self):
        """
        Return the file type of this dataset.
        """
        try:
            self.open_grib()
            return 'grib'
        except EcdissException:
            raise
        return None


class DirectoryWatch(object):
    def __init__(self, directory):
        try:
            self._inotify = inotify.adapters.Inotify()
            self._inotify.add_watch(directory)
        except:
            raise EcdissException('Something went wrong when setting up the inotify watch for %s. Does the directory exist, and do you have correct permissions?' % directory)

    def event_iterator(self):
        for event in self._inotify.event_gen():
            yield event
