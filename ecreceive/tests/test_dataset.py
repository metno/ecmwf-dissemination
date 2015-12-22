import tempfile
import datetime
import dateutil.tz
import os

import ecreceive
import ecreceive.dataset
import ecreceive.exceptions

from nose.tools import with_setup, raises

dataset = None


def setup_bogus():
    """
    Setup function: set up a bogus dataset.
    """
    global dataset
    dataset = ecreceive.dataset.Dataset('/tmp/foo')


def setup_real_files():
    """
    Setup function: set up a valid dataset with real files.
    """
    global dataset
    path = os.path.join(os.path.dirname(__file__), 'fixtures', 'BFS11120600111511001')
    dataset = ecreceive.dataset.Dataset(path)


def setup_temporary_files():
    """
    Setup function: create a named temporary file "dataset" with an md5sum file.
    """
    global dataset
    data = tempfile.NamedTemporaryFile(delete=False)
    md5 = open(data.name + '.md5', 'w+b')
    data.write('test\n')
    md5.write('d8e8fca2dc0f896fd7cb4cb0031ba249')
    dataset = ecreceive.dataset.Dataset(data.name)


def teardown_temporary_files():
    """
    Teardown function: destroy the temporary files.
    """
    os.unlink(dataset.data_path)
    os.unlink(dataset.md5_path)


def test_init_data():
    """
    Test that instantiation of a Dataset object will derive correct data and
    md5sum file paths, using a path to the data set.
    """
    dataset = ecreceive.dataset.Dataset('/tmp/foo')
    assert dataset.data_path == '/tmp/foo'
    assert dataset.md5_path == '/tmp/foo.md5'


def test_init_md5():
    """
    Test that instantiation of a Dataset object will derive correct data and
    md5sum file paths, using a path to the md5sum control file.
    """
    dataset = ecreceive.dataset.Dataset('/tmp/foo.md5')
    assert dataset.data_path == '/tmp/foo'
    assert dataset.md5_path == '/tmp/foo.md5'


@with_setup(setup_bogus)
def test_is_md5_path():
    """
    Test that the Dataset class will identify a path ending with .md5 as a
    valid md5sum file path.
    """
    assert dataset.is_md5_path('/path/to/foo/bar.md5')


@with_setup(setup_bogus)
def test_data_to_md5_path():
    """
    Test that an md5sum file path can be derived from a data file path.
    """
    assert dataset.data_to_md5_path('/path/to/foo/bar') == '/path/to/foo/bar.md5'


@with_setup(setup_bogus)
def test_md5_to_data_path():
    """
    Test that an data file path can be derived from a md5sum file path.
    """
    assert dataset.md5_to_data_path('/path/to/foo/bar.md5') == '/path/to/foo/bar'


@raises(ecreceive.exceptions.ECReceiveException)
@with_setup(setup_bogus)
def test_invalid_md5_to_data_path():
    """
    Test that deriving a data file path from an md5sum file path which does not
    end with .md5 will throw an error.
    """
    dataset.md5_to_data_path('/path/to/foo/bar.md4')


@with_setup(setup_bogus)
def test_has_no_data_file():
    """
    Test that a Dataset with a missing data file will report that correctly.
    """
    assert dataset.has_data_file() is False


@with_setup(setup_bogus)
def test_has_no_md5_file():
    """
    Test that a Dataset with a missing md5sum file will report that correctly.
    """
    assert dataset.has_md5_file() is False


@with_setup(setup_bogus)
def test_incomplete():
    """
    Test that a Dataset which is missing both the data file and the md5sum file
    is reported as incomplete.
    """
    assert dataset.complete() is False


@with_setup(setup_temporary_files)
def test_incomplete_data_missing():
    """
    Test that a Dataset which is missing the data file, but not the md5sum
    file, is reported as incomplete.
    """
    os.unlink(dataset.data_path)
    assert dataset.complete() is False
    os.unlink(dataset.md5_path)


@with_setup(setup_temporary_files)
def test_incomplete_md5_missing():
    """
    Test that a Dataset which is missing the md5sum file, but not the data
    file, is reported as incomplete.
    """
    os.unlink(dataset.md5_path)
    assert dataset.complete() is False
    os.unlink(dataset.data_path)


@with_setup(setup_real_files)
def test_has_data_file():
    """
    Test that a Dataset recognizes that its data file is on disk.
    """
    assert dataset.has_data_file()


@with_setup(setup_real_files)
def test_has_md5_file():
    """
    Test that a Dataset recognizes that its md5sum file is on disk.
    """
    assert dataset.has_md5_file()


@with_setup(setup_real_files)
def test_complete():
    """
    Test that a Dataset with both a data file and an md5sum file on disk
    reports the dataset as complete.
    """
    assert dataset.complete()


@with_setup(setup_real_files)
def test_read_md5sum():
    """
    Test that the md5sum inside the md5sum file is read and stored in the
    correct variable.
    """
    dataset.read_md5sum()
    assert dataset.md5_key == '634eece2300fef37519acec88fc6f2d8'


@raises(ecreceive.exceptions.ECReceiveException)
@with_setup(setup_bogus)
def test_read_md5sum_missing():
    """
    Test that an exception is thrown when trying to read from a non-existing
    md5sum file.
    """
    dataset.read_md5sum()


@raises(ecreceive.exceptions.InvalidDataException)
@with_setup(setup_temporary_files, teardown_temporary_files)
def test_read_md5sum_too_short():
    """
    Test that reading an md5sum file with too little data throws an exception.
    """
    with open(dataset.md5_path, 'w+b') as f:
        f.write('abcdef')
    dataset.read_md5sum()


@with_setup(setup_real_files)
def test_calculate_md5sum():
    """
    Test that the md5sum of the data file is calculated correctly.
    """
    dataset.calculate_md5sum()
    assert dataset.md5_result == '634eece2300fef37519acec88fc6f2d8'


@raises(ecreceive.exceptions.ECReceiveException)
@with_setup(setup_temporary_files)
def test_calculate_md5sum_missing():
    """
    Test that trying to calculate the md5sum of a missing file throws an
    exception.
    """
    os.unlink(dataset.data_path)
    dataset.calculate_md5sum()
    os.unlink(dataset.md5_path)


@with_setup(setup_real_files)
def test_valid():
    """
    Test that comparing a valid data file to a md5sum file returns True, and
    that the md5_key and md5_result variables are set.
    """
    assert dataset.valid()
    assert dataset.md5_key == '634eece2300fef37519acec88fc6f2d8'
    assert dataset.md5_result == dataset.md5_key


@with_setup(setup_temporary_files, teardown_temporary_files)
def test_invalid():
    """
    Test that validating a data set against a mismatching md5sum returns False.
    """
    with open(dataset.data_path, 'w+b') as f:
        f.write('invalid data')
    assert dataset.valid() is False


@with_setup(setup_temporary_files)
def test_move():
    """
    Test that moving a dataset to a different directory works.
    """
    tmp_dir = tempfile.mkdtemp()
    dataset.move(tmp_dir)
    assert dataset.data_path[:len(tmp_dir)] == tmp_dir
    assert dataset.md5_path[:len(tmp_dir)] == tmp_dir
    assert dataset.has_data_file()
    assert dataset.has_md5_file()
    os.unlink(dataset.data_path)
    os.unlink(dataset.md5_path)
    os.rmdir(tmp_dir)


@raises(OSError)
@with_setup(setup_temporary_files, teardown_temporary_files)
def test_move_nonexistent_directory():
    """
    Test that moving a dataset to a nonexistent directory throws an exception.
    """
    dataset.move('/dev/null/nonexistent/directory')


@raises(ecreceive.exceptions.ECReceiveException)
@with_setup(setup_bogus)
def test_move_incomplete_dataset():
    """
    Test that moving an incomplete dataset throws an exception.
    """
    dataset.move('/tmp')


@with_setup(setup_real_files)
def test_dataset_name():
    """
    Test that the correct dataset name is returned.
    """
    assert dataset.name() == 'BF'


@with_setup(setup_real_files)
def test_dataset_stream_use():
    """
    Test that the correct dataset stream use flag is returned.
    """
    assert dataset.stream_use() == 'S'


@with_setup(setup_real_files)
def test_dataset_version():
    """
    Test that the correct dataset version is returned.
    """
    assert dataset.version() == 1


@with_setup(setup_real_files)
def test_analysis_start_time():
    """
    Test that the correct dataset analysis time is returned.
    """
    comparison_timestamp = datetime.datetime(2015, 11, 12, 6, 0, 0, tzinfo=dateutil.tz.tzutc())
    # replacing year with 2015 since filenames do not contain the year, and the
    # current year will be used instead.
    analysis_start_time = dataset.analysis_start_time().replace(year=2015)
    assert analysis_start_time == comparison_timestamp


@with_setup(setup_real_files)
def test_analysis_end_time():
    """
    Test that the correct dataset end time is returned.
    """
    comparison_timestamp = datetime.datetime(2015, 11, 15, 11, 0, 0, tzinfo=dateutil.tz.tzutc())
    # replacing year with 2015, see above test.
    analysis_end_time = dataset.analysis_end_time().replace(year=2015)
    assert analysis_end_time == comparison_timestamp


@with_setup(setup_bogus)
@raises(ecreceive.exceptions.ECReceiveException)
def test_reference_times_missing_dataset():
    """
    Test that an exception is thrown when trying to read reference times from a
    non-existant data set.
    """
    dataset.analysis_start_time()


@with_setup(setup_real_files)
def test_file_type():
    """
    Test that the correct dataset file type is returned.
    """
    file_type = dataset.file_type()
    assert file_type == 'GRIB'
