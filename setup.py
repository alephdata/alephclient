from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(
    name='alephclient',
    version='1.3.0',
    description='Command-line client for Aleph API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Organized Crime and Corruption Reporting Project',
    author_email='data@occrp.org',
    url='http://github.com/alephdata/alephclient',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    packages=find_packages(exclude=['alephclient.tests']),
    install_requires=[
        'banal >= 0.4.2',
        'pyyaml',
        'requests >= 2.21.0',
        'requests_toolbelt >= 0.9.1',
        'click >= 7.0'
    ],
    extras_require={
        'dev': [
            'wheel',
            'pytest',
            'pytest-mock >= 1.10.0',
        ]
    },
    entry_points={
        'console_scripts': [
            'alephclient = alephclient.cli:cli'
        ],
        'memorious.operations': [
            'aleph_emit = alephclient.memorious:aleph_emit',
        ]
    },
)
