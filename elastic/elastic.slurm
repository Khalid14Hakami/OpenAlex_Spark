#!/bin/bash
#SBATCH --job-name=elasticsearch
#SBATCH --nodes=2
#SBATCH --mem-per-cpu=8GB
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=sparkjob-%j.out
# --output=sparkjob-%j.out
#SBATCH --time=01:00:00
#SBATCH --mail-user="khalid.hakami@kaust.edu.sa"
#SBATCH --mail-type=ALL
## --------------------------------------
## 0. Preparation
## --------------------------------------

# load the Spark module
#module load spark
# identify the Spark cluster with the Slurm jobid
## 1. Start the Spark cluster master
## --------------------------------------

./elasticsearch-8.5.2/bin/elasticsearch -d -p pid
sleep 310 

./elasticsearch-8.5.2/bin/elasticsearch-create-enrollment-token -s node
sleep 1800

## --------------------------------------
## 2. Start the Spark cluster workers
## --------------------------------------

srun ./elasticsearch-8.5.2/bin/elasticsearch --enrollment-token ./node -d -p pid2

## --------------------------------------
## 3. Submit a task to the Spark cluster
## --------------------------------------

## --------------------------------------

