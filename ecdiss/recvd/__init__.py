import os
import inotify.adapters

class EcdissException(Exception):
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
