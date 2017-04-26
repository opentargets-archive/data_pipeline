#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup
import mrtarget as p


setup(name=p.__pkgname__, version=p.__version__,
      description=p.__description__, author=p.__author__,
      author_email=p.__author__,
      maintainer='mkarmona',
      maintainer_email='carmona@ebi.ac.uk',
      url=p.__homepage__,
      packages=['mrtarget', 'mrtarget.common', 'mrtarget.modules'],
      license=p.__license__,
      platforms=['any'],
      install_requires=[
          'future',
          'redislite',
          'argparse==1.3.0',
          'dateutils==0.6.6',
          'decorator==3.4.2',
          'elasticsearch==2.4.0',
          'networkx==1.9.1',
          'python-dateutil==2.4.2',
          'pytz==2015.2',
          # 'requests==2.12.4',
          'requests==2.11.1',
          'six==1.9.0',
          'ujson==1.35',
          'urllib3==1.10.3',
          'iso8601>=0.1.11',
          'lxml>=3.4.4',
          'opentargets',
          'SPARQLWrapper>=1.7.6',
          'grequests',
          'paramiko==1.17.0',
          'pysftp==0.2.9',
          'jsonpickle',
          'simplejson',
          'numpy',
          'scipy',
          'tqdm==4.8.1',
          'colorlog',
          'argcomplete',
          'scikit-learn==0.17.1',
          'spacy==1.5.0',
          'nltk==3.2.1',
          'biopython==1.65',
          'rdflib',
          'colorama',
          'ftputil',
          'psutil',
          'data-model',
          'mysql-connector-python==1.0.12'
      ],
      dependency_links=[
          'git+https://github.com/opentargets/data_model.git@v1.2.5#egg=data-model',
          'https://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.0.12.tar.gz'
      ],
      include_package_data=True,
      entry_points={
          'console_scripts': ['mrtarget=mrtarget.CommandLine:main'],
      },
      data_files=[],
      scripts=[])
