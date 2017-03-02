#!/usr/bin/env python
'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''
import os
from codecs import open
from setuptools import setup, find_packages

longDescription = """pyCMR
===========


 * Created by: Manil Maskey (2016)
 * License:

----

~~~~~~~~~~~~
Requirements
~~~~~~~~~~~~

  * Python 2.7/3.2+"""

here = os.path.abspath(os.path.dirname(__file__))
__version__ = '0.1.2'

# get the dependencies and installs
with open(os.path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' not in x]


setup(
    name='pyCMR',
    version=__version__,
    author='Abdelhak Marouane',
    author_email='am0089@uah.edu',
    description='client API to ingest using CMR API',
    long_description=longDescription,
    url='https://github.com/nasa-cumulus/cmr',
    license='',
    classifiers=[
        'Framework :: Pytest',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: Freeware',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    install_requires=install_requires,
    dependency_links=dependency_links,
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)

