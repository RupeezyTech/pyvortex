import io
import os
from codecs import open
from setuptools import setup,find_packages

current_dir = os.path.abspath(os.path.dirname(__file__))

about = {}

about = {}
with open(os.path.join(current_dir, "vortex_api", "__version__.py"), "r", "utf-8") as f:
    exec(f.read(), about)

print("about hash is",about)

with io.open('README.md', 'rt', encoding='utf8') as f:
    readme = f.read()

print("keys are",about.keys)

setup(
    name=about["__name__"],
    version=about["__version__"],
    description=about["__description__"],
    long_description=readme,
    long_description_content_type='text/markdown',
    author=about["__author__"],
    author_email=about["__author_email__"],
    url=about["__url__"],
    download_url=about["__download_url__"],
    license=about["__license__"],
    packages=["vortex_api"],
    install_requires=[
        "requests>=2.25.1",
        "wrapt>=1.15.0",
        "six>=1.11.0",
        "pyOpenSSL>=17.5.0",
        "python-dateutil>=2.6.1",
        "autobahn[twisted]==19.11.2",
        "service_identity>=18.1.0"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries"
    ],
)