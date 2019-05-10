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
      # unpinned dependencies
      install_requires=[
"future",
"redislite",
#AF 15/11/18 transitive pinning to 2.x.x to solve AttributeError: 'UnixDomainSocketConnection' object has no attribute '_buffer_cutoff' 
"redis<=2.10.6",
"addict",
"envparse", #TODO remove when migration to ConfigArgParse is complete
"ConfigArgParse[yaml]",
"elasticsearch-dsl>=5.0.0,<6.0.0",
"frozendict",
"networkx",
"requests",
"jsonpickle",
"simplejson",
#when installing from GitHub, a specific commit must be used for consistency
#and to ensure dependency caching works as intended
#git+https://github.com/opentargets/ontology-utils.git@f92222b5abf89b0c3a9c2d3cd0e683676620b380#egg=opentargets-ontologyutils
"opentargets-validator>=0.4.0",
"opentargets-ontologyutils>=1.1.0",
"opentargets-urlzsource==1.0.0",
"numpy",
#used by data driven relations
"scipy",
"scikit-learn",
"biopython",
"petl",
"pyfunctional",
"pypeln<=0.1.6",
"rdflib",
"yapsy",
"lxml",
"more-itertools",
"codecov"],
      dependency_links=[],
      include_package_data=True,
      entry_points={
          'console_scripts': ['mrtarget=mrtarget.CommandLine:main'],
      },
      data_files=[],
      scripts=[])
