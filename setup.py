import os

from setuptools import find_namespace_packages, find_packages, setup

with open(os.path.join(os.path.dirname(__file__), "README.md"), "r") as fh:
    long_description = fh.read()

with open(os.path.join(os.path.dirname(__file__), "requirements.txt"), "r") as fh:
    install_requires = fh.read().split("\n")

setup(
   name='mlservicewrapper-core',
   use_scm_version = True,
   description='Configure a Python service for repeated execution',
   author='Matthew Haugen',
   author_email='mhaugen@haugenapplications.com',

   url="https://github.com/ml-service-wrapper/ml-service-wrapper-core",
   long_description=long_description,
   long_description_content_type="text/markdown",

   package_dir={"": "src"},
   packages=find_namespace_packages("src", include=['mlservicewrapper.*']),

   install_requires=install_requires,
   
   setup_requires=['setuptools_scm'],
   zip_safe=False,
   python_requires='>=3.6'
)
