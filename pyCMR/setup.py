
from distutils.core import setup





longDescription="""pyCMR
===========

**Python module - client for CMR API **

 * Created by: Manil Maskey (2016)
 * License:

----

~~~~~~~~~~~~
Requirements
~~~~~~~~~~~~

  * Python 2.7/3.2+"""



install_requires = [

    'requests',


    ]
setup(
    name='pyCMR',


    long_description=longDescription,
    version='0.1',
    py_modules=['pyCMR','Result','xmlParser'],

    url='www.python_install.com',
    license='',
    author='Abdelhak Marouane',
    author_email='am0089@uah.edu',
    description='client API to ingest using CMR API ',


)
