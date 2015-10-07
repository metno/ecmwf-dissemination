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
    data.write('test\n')
    md5.write('d8e8fca2dc0f896fd7cb4cb0031ba249')
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

@with_setup(setup_real_files)
def test_incomplete_data_missing():
    os.unlink(dataset.data_path)
    assert dataset.complete() == False
    os.unlink(dataset.md5_path)

@with_setup(setup_real_files)
def test_incomplete_md5_missing():
    os.unlink(dataset.md5_path)
    assert dataset.complete() == False
    os.unlink(dataset.data_path)

@with_setup(setup_real_files, teardown_real_files)
def test_has_data_file():
    assert dataset.has_data_file()

@with_setup(setup_real_files, teardown_real_files)
def test_has_md5_file():
    assert dataset.has_md5_file()

@with_setup(setup_real_files, teardown_real_files)
def test_complete():
    assert dataset.complete()

@with_setup(setup_real_files, teardown_real_files)
def test_read_md5sum():
    dataset.read_md5sum()
    assert dataset.md5_key == 'd8e8fca2dc0f896fd7cb4cb0031ba249'

@raises(ecdiss.recvd.EcdissException)
@with_setup(setup)
def test_read_md5sum_missing():
    dataset.read_md5sum()

@raises(ecdiss.recvd.InvalidDataException)
@with_setup(setup_real_files, teardown_real_files)
def test_read_md5sum_missing():
    with open(dataset.md5_path, 'w+b') as f:
        f.write('abcdef')
    dataset.read_md5sum()

@with_setup(setup_real_files, teardown_real_files)
def test_calculate_md5sum():
    dataset.calculate_md5sum()
    assert dataset.md5_result == 'd8e8fca2dc0f896fd7cb4cb0031ba249'

@raises(ecdiss.recvd.EcdissException)
@with_setup(setup_real_files)
def test_calculate_md5sum_missing():
    os.unlink(dataset.data_path)
    dataset.calculate_md5sum()
    os.unlink(dataset.md5_path)

@with_setup(setup_real_files, teardown_real_files)
def test_valid():
    assert dataset.valid()
    assert dataset.md5_key == 'd8e8fca2dc0f896fd7cb4cb0031ba249'
    assert dataset.md5_result == dataset.md5_key

@with_setup(setup_real_files, teardown_real_files)
def test_invalid():
    with open(dataset.data_path, 'w+b') as f:
        f.write('invalid data')
    assert dataset.valid() == False

@with_setup(setup_real_files)
def test_move():
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
@with_setup(setup_real_files, teardown_real_files)
def test_move_nonexistent_directory():
    dataset.move('/dev/null/nonexistent/directory')

@raises(ecdiss.recvd.EcdissException)
@with_setup(setup)
def test_move_incomplete_dataset():
    dataset.move('/tmp')
