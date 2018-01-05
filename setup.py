from setuptools import setup

setup(
    name='alephclient',
    version='0.1',
    description="API client for Aleph API",
    author='Journalism Development Network',
    author_email='data@occrp.org',
    url='http://github.com/alephdata/alephclient',
    license='MIT',
    classifiers=[
            "Intended Audience :: Developers",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
    ],
    py_modules=['alephclient'],
    install_requires=[
        "six==1.11.0",
        "requests==2.18.4",
        "click==6.7",
        "normality==0.5.7",
    ],
    entry_points='''
        [console_scripts]
        alephclient=alephclient.cli:cli
    ''',
)
