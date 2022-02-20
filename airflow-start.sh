#!/bin/bash
export AIRFLOW_HOME=/home/prabhakaran_mails/airflow
cd /home/prabhakaran_mails/airflow
conda activate /home/prabhakaran_mails/miniconda3/envs/airflow-medium
nohup airflow scheduler >> scheduler.log &
nohup airflow webserver -p 8080 >> webserver.log &
