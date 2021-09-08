import tempfile
import os

import ecreceive
import ecreceive.dataset
import ecreceive.exceptions

import pytest

from pymms import ProductEvent, MMSError


# for python3: from unittest.mock import MagicMock, Mock
from mock import MagicMock, Mock
from nose.tools import raises


def make_bogus_datasetpublisher(checkpoint, in_dir):
    return ecreceive.dataset.DatasetPublisher(
        checkpoint,
        "http://hei.ho/",
        in_dir,
        "http://local.mms"
    )


def setup_dirs():
    tmp_dir = tempfile.mkdtemp()
    in_dir = os.path.join(tmp_dir, "in")

    os.mkdir(in_dir)

    cp = MagicMock()
    return in_dir, cp


def test_process_only_data():
    in_dir, cp = setup_dirs()
    dsp = make_bogus_datasetpublisher(cp, in_dir)

    data_name = os.path.join(in_dir, "foo")
    with open(data_name, 'wb') as data:
        data.write(b'test\n')

    dsp.process_file(data_name)


def test_process_only_md5():
    in_dir, cp = setup_dirs()
    dsp = make_bogus_datasetpublisher(cp, in_dir)

    data_name = os.path.join(in_dir, "foo")
    md5_name = data_name + '.md5'
    with open(md5_name, 'wb') as md5:
        md5.write(b'd8e8fca2dc0f896fd7cb4cb0031ba249')  # md5sum of 'test\n'

    dsp.process_file(md5_name)


@raises(ecreceive.exceptions.InvalidFilenameException)
def test_process_bad_fileformat():
    in_dir, cp = setup_dirs()
    dsp = make_bogus_datasetpublisher(cp, in_dir)

    data_name = os.path.join(in_dir, "foo")
    md5_name = data_name + '.md5'
    with open(data_name, 'wb') as data:
        data.write(b'test\n')
    with open(md5_name, 'wb') as md5:
        md5.write(b'd8e8fca2dc0f896fd7cb4cb0031ba249')  # md5sum of 'test\n'

    dsp.process_file(md5_name)


def test_process_data(monkeypatch):
    in_dir, cp = setup_dirs()

    with monkeypatch.context() as mp:
        mp.setattr(ProductEvent, "send", lambda *a: None)

        base_url = "http://hei.ho/"
        mms_url = "http://localmms"

        dsp = ecreceive.dataset.DatasetPublisher(
            cp,
            base_url,
            in_dir,
            mms_url,
        )

        data_filename = "BFS11120600111511001"
        data_name = os.path.join(in_dir, data_filename)
        md5_name = data_name + '.md5'
        with open(data_name, 'wb') as data:
            data.write(b'test\n')
        with open(md5_name, 'wb') as md5:
            md5.write(b'd8e8fca2dc0f896fd7cb4cb0031ba249')  # md5sum of 'test\n'

        dsp.process_file(md5_name)


class MyProblem(Exception):
    pass


class NotMyProblem(Exception):
    pass


class FailRepeatedly(object):
    def __init__(self, n_fail, exception=MyProblem):
        self.n_fail = n_fail
        self.count = 0
        self.exception = exception

    def __call__(self):
        self.count += 1
        if self.count <= self.n_fail:
            raise self.exception()


def test_retry_0():
    f = FailRepeatedly(0)
    ecreceive.retry_n(f, interval=0.01, exceptions=(MyProblem,), warning=1, error=2, give_up=3)
    assert f.count == 1


def test_retry_1():
    f = FailRepeatedly(1)
    ecreceive.retry_n(f, interval=0.01, exceptions=(MyProblem,), warning=1, error=2, give_up=3)
    assert f.count == 2


def test_retry_10():
    f = FailRepeatedly(10)
    ecreceive.retry_n(f, interval=0.01, exceptions=(MyProblem,), warning=1, error=2, give_up=3)
    assert f.count == 3


@raises(NotMyProblem)
def test_retry_other():
    f = FailRepeatedly(10, NotMyProblem)
    ecreceive.retry_n(f, interval=0.01, exceptions=(MyProblem,), warning=1, error=2, give_up=3)


def test_retry_indefinitely():
    f = FailRepeatedly(10)
    ecreceive.retry_n(f, interval=0.01, exceptions=(MyProblem,), warning=1, error=2, give_up=-1)
    assert f.count == 11
