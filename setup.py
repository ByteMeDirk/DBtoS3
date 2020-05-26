import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requires = [
    'boto3',
    'numpy',
    'psycopg2',
    'pandas'
]

setuptools.setup(
    name='dbtos3',
    version='0.0.2-alpha',
    description='Replication & Full Load Application for multiple databases to s3',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='DirkSCGM',
    author_email='dirkscgm@gmail.com',
    url='https://github.com/DirksCGM/DBtoS3',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    packages=setuptools.find_packages(),
    install_requires=requires,
    python_requires='>=3.6',
    keywords=['postgres', 's3', 'aws', 'mysql']
)
