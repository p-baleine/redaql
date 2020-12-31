import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(here, 'redaql', '__version__.py'), 'r') as f:
    exec(f.read(), about)

packages = ['redaql']

setup(
    version=about['__VERSION__'],
    packages=packages,
)
