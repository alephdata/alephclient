from setuptools import setup, find_packages

setup(
    name='alephclient',
    version='0.8.0',
    description='Command-line client for Aleph API',
    author='Journalism Development Network',
    author_email='data@occrp.org',
    url='http://github.com/alephdata/alephclient',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    packages=find_packages(exclude=['alephclient.tests']),
    install_requires=[
        'six>=1.11.0',
        'banal',
        'pyyaml',
        'requests>=2.18.4',
        'requests_toolbelt>=0.8.0',
        'click>=6.7',
        'normality>=0.6.1',
        'grpcio>=1.11.0',
        'pathlib2>=2.3.2'
    ],
    extras_require={
        # ':python_version < "3.4"': [
        #     'pathlib2>=2.3.2'
        # ],
        'dev': [
            'pytest',
            'pytest-mock>=1.10.0',
            'grpcio-tools>=1.11.0'
        ]
    },
    entry_points={
        'console_scripts': [
            'alephclient = alephclient.cli:cli'
        ]
    },
)
