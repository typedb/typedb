from setuptools import setup, PEP420PackageFinder 

pep420_package_finder = PEP420PackageFinder()

setup(
    name='grakn',
    packages=pep420_package_finder.find('.', include=['grakn*']),
    version='1.4.2',
    license='Apache-2.0',
    description='A Python client for Grakn',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author='Grakn Labs',
    author_email='community@grakn.ai',
    url='https://github.com/graknlabs/grakn/tree/master/client-python',
    keywords=['grakn', 'database', 'graph', 'knowledgebase', 'knowledge-engineering'],
    python_requires='>=3.6.0',
    install_requires=['grpcio', 'protobuf']
)
