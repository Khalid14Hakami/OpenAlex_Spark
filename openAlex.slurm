#!/bin/bash
#SBATCH --job-name=spark_openAlex_all_no_schema
#SBATCH --nodes=4
#SBATCH --mem-per-cpu=64GB
#SBATCH --cpus-per-task=8
#SBATCH --ntasks-per-node=1
#SBATCH --output=openalex-%j.out
# --output=sparkjob-%j.out
#SBATCH --time=01:00:00
#SBATCH --mail-user="khalid.hakami@kaust.edu.sa"
#SBATCH --mail-type=ALL

# load the Spark module
module load spark/2.2.1-bin-hadoop2.6
module load pyspark/2.2.0 
# identify the Spark cluster with the Slurm jobid
export SPARK_IDENT_STRING=$SLURM_JOBID

# prepare directories
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR:-$HOME/.spark/worker}
export SPARK_LOG_DIR=${SPARK_LOG_DIR:-$HOME/.spark/logs}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:-/tmp/spark}
mkdir -p $SPARK_LOG_DIR $SPARK_WORKER_DIR

## start spark 

start-master.sh
sleep 1
MASTER_URL=$(grep -Po '(?=spark://).*' \
             $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.*master*.out)
echo "${MASTER_URL}" > "${SPARK_WORKER_DIR}/${SPARK_IDENT_STRING}_spark_master"

# start spark  workers

# start the workers on each node allocated to the tjob
export SPARK_NO_DAEMONIZE=1
srun  --output=$SPARK_LOG_DIR/spark-%j-workers.out --label \
      start-slave.sh ${MASTER_URL} &

# submit spark job
spark-submit --master ${MASTER_URL} --num-executors 30 --executor-cores 8 --executor-memory 20g --driver-memory 16g --conf spark.driver.maxResultSize=8g ./openAlex4.py

scancel ${SLURM_JOBID}.0

# stop the master
stop-master.sh
