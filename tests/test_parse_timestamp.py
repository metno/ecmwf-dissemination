from ecreceive import force_utc, parse_filename_timestamp

import datetime
import dateutil.tz

# from nose.tools import with_setup, raises
# from unittest.case import SkipTest


def test_force_utc():
    """
    Test that a naive timestamp is successfully converted into UTC.
    """
    naive_timestamp = datetime.datetime(2000, 1, 1, 12, 0, 0)
    utc_timestamp = force_utc(naive_timestamp)
    assert isinstance(utc_timestamp.tzinfo, dateutil.tz.tzutc)


def test_force_utc_with_timezone():
    """
    Test that a timestamp with timezone is untouched by UTC conversion.
    """
    timezone = dateutil.tz.gettz('GMT+08:00')
    timestamp_source = datetime.datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone)
    timestamp_dest = force_utc(timestamp_source)
    delta = timestamp_source - timestamp_dest
    assert delta.total_seconds() == 0


def test_parse_filename_timestamp_current_year():
    """
    Test that the timestamp parser works correctly when parsing timestamps from
    the current year.
    """
    now = datetime.datetime(2015, 12, 1, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    comparison_timestamp = datetime.datetime(2015, 6, 1, 13, 25, 0, tzinfo=dateutil.tz.tzutc())
    timestamp = parse_filename_timestamp('06011325', now)
    assert timestamp == comparison_timestamp


def test_parse_filename_timestamp_previous_year():
    """
    Test that the timestamp parser works correctly when parsing timestamps from
    the previous year.
    """
    now = datetime.datetime(2015, 1, 1, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    comparison_timestamp = datetime.datetime(2014, 12, 1, 13, 25, 0, tzinfo=dateutil.tz.tzutc())
    timestamp = parse_filename_timestamp('12011325', now)
    assert timestamp == comparison_timestamp


def test_parse_filename_timestamp_next_year():
    """
    Test that the timestamp year is derived correctly for cases when the date of the forecast
    is in the next month compared to the current date, and in the next year.
    """
    now = datetime.datetime(2015, 12, 12, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    comparison_timestamp = datetime.datetime(2016, 1, 6, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    timestamp = parse_filename_timestamp('01060000', now)
    assert timestamp == comparison_timestamp


def test_parse_filename_timestamp_leap_year():
    """
    Test that the timestamp parser works correctly when parsing a leap year
    with the date of February 29th.
    """
    now = datetime.datetime(2016, 2, 1, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    comparison_timestamp = datetime.datetime(2016, 2, 29, 3, 0, 0, tzinfo=dateutil.tz.tzutc())
    timestamp = parse_filename_timestamp('02290300', now)
    assert timestamp == comparison_timestamp


def test_parse_filename_timestamp_missing_time():
    """
    Test that the timestamp parser works with timestamps that are missing hour
    and minute.
    """
    now = datetime.datetime(2015, 12, 1, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    comparison_timestamp = datetime.datetime(2015, 6, 1, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    timestamp = parse_filename_timestamp('0601____', now)
    assert timestamp == comparison_timestamp


def test_parse_filename_timestamp_missing_everything():
    """
    Test that the timestamp parser works with only underscores.
    """
    timestamp = parse_filename_timestamp('________', datetime.datetime.now())
    assert timestamp is None
