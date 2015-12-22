try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'ECReceive daemon',
    'author': 'IT-GEO-TF',
    'url': 'https://github.com/metno/ecmwf-dissemination',
    'download_url': 'https://github.com/metno/ecmwf-dissemination',
    'author_email': 'it-geo-tf@met.no',
    'version': '0.1',
    'install_requires': [
        'nose==1.3.7',
        'inotify==0.2.4',
        'python-dateutil==2.4.2',
        'productstatus-client==3.0.0',
        'mock==1.3.0',
        'funcsigs==0.4',
        'pyzmq==15.1.0',
    ],
    'packages': [],
    'scripts': [],
    'name': 'ecreceive'
}

setup(**config)
