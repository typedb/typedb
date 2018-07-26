from setuptools import setup

setup(
    name='grakn',
    packages=['grakn'],
    version='1.2',
    license='Apache-2.0',
    description='A Python client for Grakn',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author='Grakn Labs',
    author_email='community@grakn.ai',
    url='https://github.com/flyingsilverfin/grakn/tree/client-python-dev',
#    download_url='https://github.com/graknlabs/grakn-python/archive/v0.8.1.tar.gz',
    keywords=['grakn', 'database', 'graph', 'knowledgebase', 'knowledge-engineering'],
    python_requires='>=3.6.0',
    install_requires=['grpcio']
)
