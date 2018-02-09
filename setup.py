from setuptools import setup

setup(
    name='alephclient',
    version='0.3.1',
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
        "six>=1.11.0",
        "banal",
        "requests>=2.18.4",
        "requests_toolbelt>=0.8.0",
        "click>=6.7",
        "normality>=0.5.8",
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
