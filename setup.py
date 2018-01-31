from setuptools import setup

setup(
    name='alephclient',
    version='0.2',
    description="Command-line client for Aleph API",
    author='Journalism Development Network',
    author_email='data@occrp.org',
    url='http://github.com/alephdata/alephclient',
    license='MIT',
    classifiers=[
            "Intended Audience :: Developers",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
    ],
    packages=['alephclient', 'alephclient.tasks'],
    install_requires=[
        "six==1.11.0",
        "requests==2.18.4",
        "click==6.7",
        "normality==0.5.7",
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-mock',
        ]
    },
    entry_points='''
        [console_scripts]
        alephclient = alephclient.cli:cli
    ''',
)
