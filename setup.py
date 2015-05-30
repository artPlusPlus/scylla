"""
Scylla
------

Scylla is a framework for distributing dependency graphs.
"""

from setuptools import setup

setup(
    name='scylla',
    version='0.3.1',
    url='https://github.com/artPlusPlus/scylla',
    license='LGPLv3',
    author='Matt Robinson',
    author_email='matt@technicalartisan.com',
    description='A framework for distributing dependency graphs',
    long_description=__doc__,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='zmq dependency graph',
    packages=['scylla'],
    install_requires=[
        'pyzmq',

    ],
    extras_require={
        'test': ['py.test']
    }
)
