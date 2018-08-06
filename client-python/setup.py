from setuptools import setup, PEP420PackageFinder 

pep420_package_finder = PEP420PackageFinder()

setup(
    name='grakn',
    packages=pep420_package_finder.find('.', include=['grakn*']),
    version='1.2.4.5',
    license='Apache-2.0',
    description='A Python client for Grakn',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author='Grakn Labs',
    author_email='community@grakn.ai',
    url='https://github.com/flyingsilverfin/grakn/tree/client-python-dev',
#    download_url='https://github.com/graknlabs/grakn-python/archive/v0.8.1.tar.gz',
    keywords=['grakn', 'database', 'graph', 'knowledgebase', 'knowledge-engineering'],
    python_requires='>=3.4.0',
    install_requires=['grpcio', 'protobuf']
)
