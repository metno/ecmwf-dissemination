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
    'install_requires': ['nose', 'inotify'],
    'packages': [],
    'scripts': [],
    'name': 'ecdiss'
}

setup(**config)
