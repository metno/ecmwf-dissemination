# ECMWF dissemination daemons

## Setting up a development environment

First, you'll need some dependencies to build the pygrib library:

```
sudo apt-get install python-virtualenv python-pip
sudo apt-get install build-essential libjasper-dev libopenjpeg-dev python-dev libxml2-dev libxslt-dev libgrib-api-dev zlib1g-dev libpng12-dev
```

Change to the repository you just checked out.

Now, create a virtual environment, and activate it:

```
virtualenv deps
source deps/bin/activate
```

Next, install the ECMWF daemons and their dependencies in the virtual environment:

```
pip install numpy
pip install -e .
```

Check that the tests pass, then you're done.

```
nosetests
```
