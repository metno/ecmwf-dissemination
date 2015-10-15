try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'ECDISS daemons',
    'author': 'IT-GEO-TF',
    'url': 'https://gitlab.met.no/it-geo/ecmwf-dissemination',
    'download_url': 'https://gitlab.met.no/it-geo/ecmwf-dissemination',
    'author_email': 'it-geo-tf@met.no',
    'version': '0.1',
    'install_requires': [
        'nose==1.3.7',
        'inotify==0.2.4',
        'python-dateutil==2.4.2',
        'modelstatus-client==1.1.0',
        'numpy==1.10.1',
        'pygrib==2.0.0',
        'pyproj==1.9.4',
    ],
    'packages': [],
    'scripts': [],
    'name': 'ecdiss'
}

setup(**config)
