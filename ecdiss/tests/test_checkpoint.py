from ecdiss.recvd.daemon import Checkpoint

import tempfile

from nose.tools import raises  # , with_setup
# from unittest.case import SkipTest


def setup_with_tempfile(json):
    tmpfile = tempfile.NamedTemporaryFile()
    if json is not None:
        with open(tmpfile.name, 'w+b') as cp:
            cp.write(json)
    return tmpfile, Checkpoint(tmpfile.name)


@raises(IOError)
def test_bogus_file_add():
    """
    Test a Checkpoint with an invalid path
    raises an exception when making changes.
    """
    c = Checkpoint('/this/is/no/file')
    assert not c.keys()
    c.add('my-favourite-key', 1)


def test_bogus_file_delete():
    """
    Test a Checkpoint with an invalid path
    raises an exception when adding a flag.
    """
    c = Checkpoint('/this/is/no/file')
    assert not c.keys()
    c.delete('my-favourite-key')


def test_load_missing():
    """
    Test that loading a missing checkpoint file does not yield any keys
    and no errors.
    """
    tmpfile, checkpoint = setup_with_tempfile(None)
    assert not checkpoint.keys()


def test_load_empty():
    """
    Test that loading an empty checkpoint file does not yield any keys
    and no errors.
    """
    tmpfile, checkpoint = setup_with_tempfile('{}')
    assert not checkpoint.keys()
    assert checkpoint.get('x') == 0


def test_load_data():
    """
    Test that loading an empty checkpoint file does not yield any keys
    and no errors.
    """
    tmpfile, checkpoint = setup_with_tempfile('{ "a" : 1, "b" : 2 }')
    assert checkpoint.get('a') == 1
    assert checkpoint.get('b') == 2
    assert checkpoint.get('c') == 0


def test_add_save():
    """
    Test that adding to a checkpoint stores the new state in memory and file.
    """
    tmpfile, checkpoint = setup_with_tempfile('{ "a" : 1, "b" : 2 }')
    assert checkpoint.get('a') == 1
    checkpoint.add('a', 1)
    assert checkpoint.get('a') == 1
    checkpoint.add('a', 2)
    assert checkpoint.get('a') == 3

    cp_reload = Checkpoint(tmpfile.name)
    assert cp_reload.get('a') == 3


def test_delete_save():
    """
    Test that deleting a key from a checkpoint object stores the new state in memory and file.
    """
    tmpfile, checkpoint = setup_with_tempfile('{"a": 1}')
    assert checkpoint.get('a') == 1

    checkpoint.delete('a')
    cp_reload = Checkpoint(tmpfile.name)
    assert not cp_reload.keys()

    assert checkpoint.get('a') == 0


def test_get_nosave():
    """
    Test that get for an unknown key does not stores new state in memory or file.
    """
    tmpfile, checkpoint = setup_with_tempfile('{"a": 1}')
    assert checkpoint.get('b') == 0

    cp_reload = Checkpoint(tmpfile.name)
    assert 'b' not in cp_reload.keys()
