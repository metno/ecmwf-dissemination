# ECMWF dissemination daemons

## Setting up a development environment

First, you'll need some dependencies to build the pygrib library:

```
sudo apt-get install libjasper-dev libopenjpeg-dev build-essential
```

Change to the repository you just checked out.

Now, create a virtual environment, and activate it:

```
virtualenv deps
source deps/bin/activate
```

Next, install the ECMWF daemons and their dependencies in the virtual environment:

```
python setup.py develop
```

Check that the tests pass, then you're done.

```
nosetests
```