import os

from setuptools import setup, find_packages

with open('README.md') as f:
    README = f.read()

with open('LICENSE') as f:
    LICENSE = f.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

install_requires = ['requests >= 1.5.5', 'slugify']

setup(
    name='metabasepy',
    version='1.2.0',
    use_scm_version=True,
    setup_requires=['setuptools_scm', 'future'],
    description='metabase api python client',
    long_description=README,
    author='mertsalik',
    author_email='salik@itu.edu.tr',
    url='https://github.com/mertsalik/metabasepy',
    license=LICENSE,
    packages=find_packages(exclude=['tests', 'docs']),
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
