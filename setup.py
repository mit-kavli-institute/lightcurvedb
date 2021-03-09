import setuptools
import codecs
import os.path


# See https://packaging.python.org/guides/single-sourcing-package-version/
def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


with open('README.md', 'r') as fh:
    long_description = fh.read()

with open('requirements.txt', 'rt') as f:
    requirements = [l.strip() for l in f.readlines()]

with open('./docs/requirements.txt', 'rt') as f:
    doc_requirements = [l.strip() for l in f.readlines()]

setuptools.setup(
    name='lightcurvedb',
    version=get_version('lightcurvedb/__init__.py'),
    author='William Fong',
    author_email='willfong@mit.edu',
    install_package_data=True,
    entry_points='''
    [console_scripts]
    lcdb=lightcurvedb.cli:lcdbcli
    ''',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
    install_requires=requirements,
    extras_require={"docs": doc_requirements}
)
