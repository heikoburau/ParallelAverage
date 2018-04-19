#!/bin/bash

JOB_DIR=$1
PACKAGE_DIR=$2

mkdir -p $JOB_DIR

qsub $3 -wd $JOB_DIR $PACKAGE_DIR/job_script.sh $PACKAGE_DIR/run_task.py
