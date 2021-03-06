#!/usr/bin/env bash

#########################################################################################
############################ Environment Configs ########################################
#########################################################################################

# Python3.6 path
export PYSPARK_PYTHON=/usr/bin/python3

# export CerebralCortex path
export PYTHONPATH="${PYTHONPATH}:/MAIN-DIR-PATH/CerebralCortex-Kernel-Examples/"

#Spark path, uncomment if spark home is not exported else where.
export SPARK_HOME=/MAIN-DIR-PATH/spark-2.4.0-bin-hadoop2.7

# spark master. This will work on local machine only. In case of cloud, provide spark master node URL:port.
SPARK_MASTER="local[*]"

# add -p $PARTICIPANTS at the end of below command if participants' UUIDs are provided
spark-submit main.py