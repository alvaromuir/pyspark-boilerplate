#!/usr/bin/python

# -*- coding: utf-8 -*-
"""
    dova.main
    ~~~~~~~~~
    This module implements the central RCA application object.

"""
__author__ = "Alvaro Muir"
__copyright__ = "Copyright 2018, VZ IT Analytics"
__credits__ = ["Vz IT Analytics Data Engineering"]
__license__ = "Vz Use Only"
__version__ = "0.1"
__maintainer__ = "Alvaro Muir"
__email__ = "alvaro.muir@verizon.com"
__status__ = "Development"

import argparse
import importlib
import time
import os
import sys
import pyspark

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


from pyspark.sql import SparkSession

def create_spark_session(app_name):
    spark_session = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("WARN")

    return spark_session

# pylint:disable=E0401

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*', help="Add'l args to send to the PySpark job (ex: --job-args template=model-1 foo=bar")
    parser.add_argument('--app-prefix', type=str, dest='app_prefix', default='PySpark Boilerplate - ',
                        help='Adds a prefix if desired to the job name for identification in the history server. (ex: PySpark Boilerplate - ')

    args = parser.parse_args()
    print("Called with arguments: %s" % args)

    environment = {'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''}

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print('job_args_tuples: %s' % job_args_tuples)
        job_args = {a[0]: a[1] for a in job_args_tuples}

    app_name = args.app_prefix + args.job_name
    print('\nRunning job %s, identified as \'%s\'...\nenvironment is %s\n' % (args.job_name, app_name, environment))

    # print(environment)
    os.environ.update(environment)
    sc = pyspark.SparkContext(appName=app_name, environment=environment)
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    start = time.time()
    job_module.analyze(sc, **job_args)
    end = time.time()

    print("\nExecution of job %s took %s seconds" % (args.job_name, end-start))
