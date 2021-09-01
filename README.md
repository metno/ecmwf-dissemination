# ECReceive

Daemon for robust receival of ECMWF dissemination data sets.

## Setting up a development environment

We recommend to develop using Python virtual environment:

```bash
virtualenv deps
source deps/bin/activate
```

or using a virtualenv wrapper (https://virtualenvwrapper.readthedocs.io/en/latest/):

```bash
mkvirtualenv ecmwf-dissemination
```

Install dependencies
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Next, install the ECMWF daemons and their dependencies in the virtual environment:

```bash
pip install -e .
```

Check that the tests pass, then you're done.

```bash
nosetests
```

## Internal messaging

ECReceive uses the TCP ports `9960`, `9970`, `9980` and `9990` on `127.0.0.1` for internal communication. They must not be used by any other program running on the same server.
