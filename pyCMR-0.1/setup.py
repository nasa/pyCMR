#!/usr/bin/env python
#
#  Copyright (c) 2016, GHRC DAAC
#
#  license: GNU LGPL
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#


"""setup/install script for CMR client API"""


import os
from distutils.core import setup

from cmr import __version__

this_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_dir, 'README.rst')) as f:
    LONG_DESCRIPTION = '\n' + f.read()

setup(
    name='pyCMR',
    version=__version__,
    py_modules=['pyCMR'],
    author='Manil Maskey',
    author_email='manil.maskey@nasa.gov',
    description='client API to search and download data using CMR',
    long_description=LONG_DESCRIPTION,
    url='http://ghrc.nsstc.nasa.gov',
    download_url='',
    keywords='ghrc nasa'.split(),
    license='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: NASA, Researchers, App Developers',
        'License ::  :: ',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Research :: Education',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
    ]
)
