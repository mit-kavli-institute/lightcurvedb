import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='lightcurvedb',
    version='0.0.3',
    author='William Fong',
    author_email='willfong@mit.edu',
    install_package_data=True,
    entry_points='''
    [console_scripts]
    lcdb=lightcurvedb.cli.base:lcdbcli
    ''',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
)
