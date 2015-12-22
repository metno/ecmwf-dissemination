# ECReceive

Daemon for robust receival of ECMWF dissemination data sets.

## Setting up a development environment

First, you'll need some dependencies to build the pygrib library:

```
sudo apt-get install python-virtualenv python-pip build-essential python-dev libxml2-dev libxslt-dev
```

Change to the repository you just checked out.

Now, create a virtual environment, and activate it:

```
virtualenv deps
source deps/bin/activate
```

Next, install the ECMWF daemons and their dependencies in the virtual environment:

```
pip install -e .
```

Check that the tests pass, then you're done.

```
nosetests
```

## Internal messaging

ECReceive uses the TCP ports `9960`, `9970`, `9980` and `9990` on `127.0.0.1` for internal communication. They must not be used by any other program running on the same server.
