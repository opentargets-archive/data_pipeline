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
      ],
      dependency_links=[
      ],
      include_package_data=True,
      entry_points={
          'console_scripts': ['mrtarget=mrtarget.CommandLine:main'],
      },
      data_files=[],
      scripts=[])
