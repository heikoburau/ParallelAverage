#!/bin/bash -i 
#$ -S /bin/bash 
# 
# MPI-PKS script for job submission script with ’qsub’. 
# Syntax is Bash with special qsub-instructions that begin with ’#$’. 
# For more detailed documentation, see 
#     https://start.pks.mpg.de/dokuwiki/doku.php/getting-started:queueing_system 
# 
 
 
# --- Mandatory qsub arguments 
# Hardware requirements. 
#$ -l h_rss=256M,h_fsize=10M,h_cpu=12:00:00,hw=x86_64
#$ -j n 
 
# --- Job Execution 
# For faster disk access copy files to /scratch first. 

WORK_DIR=./$SGE_TASK_ID

mkdir -p $WORK_DIR

module purge
module load intelpython3

cd $WORK_DIR
python $1 $SGE_TASK_ID
