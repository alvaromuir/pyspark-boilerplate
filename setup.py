from setuptools import setup

setup(
    name='PySpark-Boilerplate',
    version='0.0.1',
    packages=['src.jobs', 'src.jobs.wordcount', 'src.shared', 'tests', 'tests._jobs', 'tests._jobs.wordcount'],
    url='',
    license='',
    author='Alvaro Muir',
    author_email='alvaro.muir@one.verizon.copm',
    description='PySpark Boilerplate code for cluster deployment'
)
