"""
Create by yy on 2019/9/21
tool-yy
-------------

psql-yy is used to get some help and quickly connect the database from anywhere.

I'm looking for a good job, if you can, please contact me by email "1023767856@qq.com".

Links
`````

* `development version <https://github.com/guaidashu/psql_yy>`_
"""
import os
import sys

from setuptools import setup, find_packages

about = {}

with open('psql_yy/__about__.py') as f:
    exec(f.read(), about)

if sys.argv[-1] == 'test':
    status = os.system('make check')
    status >>= 8
    sys.exit(status)

setup(
    name=about['__title__'],
    version=about['__version__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    description=about['__description__'],
    long_description=__doc__,
    long_description_content_type="text/markdown",
    license=about['__license__'],
    url=about['__url__'],
    packages=find_packages(),
    install_requires=about['__install_requires__'],
    classifiers=[
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ]
)
