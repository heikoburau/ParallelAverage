#!/bin/bash

JOB_DIR=$1
PACKAGE_DIR=$2

# edit only here:

module purge
module load intelpython3

######################

cd $JOB_DIR
python $PACKAGE_DIR/legacy/average_collector.py
