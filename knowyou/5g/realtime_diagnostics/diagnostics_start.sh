#!/bin/bash

SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)
START_DATA=`date +%Y%m%d`

# 运行spark实时流
# spark-submit --jars ${SHELL_FOLDER}/spark-sql-kafka-0-10_2.11-2.3.0.jar ${SHELL_FOLDER}/kafka_read.py >> ${SHELL_FOLDER}/log/read_${START_DATA}.log 2>&1 &
python3 ${SHELL_FOLDER}/kafka_read.py >> ${SHELL_FOLDER}/log/read_${START_DATA}.log 2>&1 &

sleep 2m

while [ true ]; do
    while [ true ]; do
        old_size=`du -b ${SHELL_FOLDER}/kafka_train_df/*.csv | awk 'BEGIN{size=0}{size=size+$1} END{print size}'`
        sleep 10s
        new_size=`du -b ${SHELL_FOLDER}/kafka_train_df/*.csv | awk 'BEGIN{size=0}{size=size+$1} END{print size}'`
        if [ ${old_size} -eq ${new_size} ]; then
            break
        fi
    done

    # 运行批处理
    # spark-submit --jars ${SHELL_FOLDER}/spark-sql-kafka-0-10_2.11-2.3.0.jar ${SHELL_FOLDER}/kafka_write.py >> ${SHELL_FOLDER}/log/write_${START_DATA}.log 2>&1 &
    python3 ${SHELL_FOLDER}/kafka_write.py >> ${SHELL_FOLDER}/log/write_${START_DATA}.log 2>&1 &
    echo '运行批处理success'

    sleep 10m
done
