#!/bin/bash

JOB_DIR=$1
PACKAGE_DIR=$2

module load intelpython3

cd $JOB_DIR
python $PACKAGE_DIR/average_collector.py
