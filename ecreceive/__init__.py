import time
import copy
import logging
import traceback
import datetime
import dateutil.tz
import dateutil.relativedelta


def force_utc(timestamp):
    """
    Force a "naive" timestamp into UTC, or return the original timestamp
    for sane timestamps.
    """
    if not timestamp.tzinfo:
        return timestamp.replace(tzinfo=dateutil.tz.tzutc())
    return timestamp


def parse_filename_timestamp(stamp, now):
    """
    Parse a timestamp from a ECMWF dataset filename.
    The year is not part of the filename, and must be guessed.
    Returns a datetime object, or None if only underscores are given.
    """
    if stamp == '________':
        return None
    stamp = '%d%s' % (now.year, stamp)
    try:
        ts = datetime.datetime.strptime(stamp, '%Y%m%d%H%M')
    except ValueError:
        ts = datetime.datetime.strptime(stamp, '%Y%m%d____')
    ts = force_utc(ts)

    # Check whether the dataset timestamp contains a different month than the
    # current month. If so, it may be set in a different year, which is not
    # specified in the timestamp. This is a workaround for that, assuring that
    # the correct year is used.
    for delta in [-1, 1]:
        ts_alternate = copy.copy(now)
        ts_alternate += dateutil.relativedelta.relativedelta(months=delta)
        if ts_alternate.month == ts.month:
            ts = ts.replace(year=ts_alternate.year)
    return ts


def run_with_exception_logging(func):
    """
    Run a function and catch all exceptions while logging them.
    """
    try:
        exit_code = func()
    except Exception as e:
        logging.critical("Fatal error: %s" % e)
        exception = traceback.format_exc().split("\n")
        logging.debug("***********************************************************")
        logging.debug("Uncaught exception during program execution. THIS IS A BUG!")
        logging.debug("***********************************************************")
        for line in exception:
            logging.debug(line)
        exit_code = 255

    return exit_code


def retry_n(func, interval=5, exceptions=(Exception,), warning=1, error=3, give_up=5):
    """
    Call 'func' and, if it throws anything listed in 'exceptions', catch it and retry again
    up to 'give_up' times. If give_up is <= 0, retry indefinitely.
    Checks that error > warning > 0, and give_up > error or give_up <= 0.
    """
    assert (warning > 0) and (error > warning) and (give_up <= 0 or give_up > error)
    tries = 0
    while True:
        try:
            return func()
        except exceptions as e:
            tries += 1
            if give_up > 0 and tries >= give_up:
                logging.error('Action failed %d times, giving up: %s' % (give_up, e))
                return False
            if tries >= error:
                logfunc = logging.error
            elif tries >= warning:
                logfunc = logging.warning
            else:
                logfunc = logging.info
            logfunc('Action failed, retrying in %d seconds: %s' % (interval, e))
            time.sleep(interval)
