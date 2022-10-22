from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '2.0'
DESCRIPTION = 'Utility for running workflows leveraging delta live tables from interactive notebooks'

# Setting up
setup(
    name="dlt_with_debug",
    version=VERSION,
    author="Souvik Pratiher",
    author_email="souvik.pratiher@databricks.com",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    keywords=['python3', 'delta live tables'],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3"
    ]
)