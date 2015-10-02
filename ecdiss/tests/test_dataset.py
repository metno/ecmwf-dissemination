import ecdiss.recvd
import tempfile
import os

from nose.tools import *

dataset = None

def setup():
    global dataset
    dataset = ecdiss.recvd.Dataset('/tmp/foo')

def setup_real_files():
    global dataset
    data = tempfile.NamedTemporaryFile(delete=False)
    md5 = open(data.name + '.md5', 'w+b')
    data.write('\0')
    md5.write('\0')
    dataset = ecdiss.recvd.Dataset(data.name)

def teardown_real_files():
    os.unlink(dataset.data_path)
    os.unlink(dataset.md5_path)

def test_init_data():
    dataset = ecdiss.recvd.Dataset('/tmp/foo')
    assert dataset.data_path == '/tmp/foo'
    assert dataset.md5_path == '/tmp/foo.md5'

def test_init_md5():
    dataset = ecdiss.recvd.Dataset('/tmp/foo.md5')
    assert dataset.data_path == '/tmp/foo'
    assert dataset.md5_path == '/tmp/foo.md5'

@with_setup(setup)
def test_is_md5_path():
    assert dataset.is_md5_path('/path/to/foo/bar.md5')

@with_setup(setup)
def test_data_to_md5_path():
    assert dataset.data_to_md5_path('/path/to/foo/bar') == '/path/to/foo/bar.md5'

@with_setup(setup)
def test_md5_to_data_path():
    assert dataset.md5_to_data_path('/path/to/foo/bar.md5') == '/path/to/foo/bar'

@with_setup(setup)
def test_has_no_data_file():
    assert dataset.has_data_file() == False

@with_setup(setup)
def test_has_no_md5_file():
    assert dataset.has_md5_file() == False

@with_setup(setup)
def test_incomplete():
    assert dataset.complete() == False

@with_setup(setup_real_files, teardown_real_files)
def test_has_data_file():
    assert dataset.has_data_file()

@with_setup(setup_real_files, teardown_real_files)
def test_has_md5_file():
    assert dataset.has_md5_file()

@with_setup(setup_real_files, teardown_real_files)
def test_complete():
    assert dataset.complete()
