try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    "description": "ECReceive daemon",
    "author": "IT-GEO-TF",
    "url": "https://github.com/metno/ecmwf-dissemination",
    "download_url": "https://github.com/metno/ecmwf-dissemination",
    "author_email": "it-geo-tf@met.no",
    "version": "1.0.0",
    "install_requires": [
        "nose==1.3.7",
        "inotify==0.2.4",
        "python-dateutil==2.5.0",
        "productstatus-client==5.1.0",
        "mock==1.3.0",
        "funcsigs==0.4",
    ],
    "packages": [],
    "scripts": [],
    "name": "ecreceive",
}

setup(**config)
