from setuptools import setup, find_packages
import os
import re


ROOT = os.path.dirname(__file__)
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+([0-9]+[a-b][0-9]+)*)['"]''')


def get_version():
    init = open(os.path.join(ROOT, 'houzz', 'common', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)


with open('README.md') as f:
    long_desc = f.read()

setup(
    name="houzz",
    url="https://github.com/davidyang-houzz/houzz-common-lib.git",
    version="1.0.0",
    maintainer="david yang",
    description="A Python library for houzz developer access.",
    packages=find_packages(),
    long_description=long_desc,
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    install_requires=[
        "pyyaml",
        "six"
    ],
    include_package_data=True
)