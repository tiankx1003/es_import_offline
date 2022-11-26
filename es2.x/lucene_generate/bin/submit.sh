#!/bin/bash
dt=20991231
index=es_import_offline_20991231
type=datum
alias=es_import_offline
pk=pri_key
es=pri_key,value01,value02
hive=pri_key,value01,value02
export HADOOP_USER_NAME=tiankx
/opt/module/spark-3.3.0/bin/spark-submit \
    --master "local[*]" \
    --executor-memory=1g \
    --class io.github.tiankx1003.LuceneGen \
    /home/tiankx/lucene_generate-1.1.20221002_alpha.jar \
    --zookeeper omega \
    --chroot /es_import_offline \
    --hive-table tmp.es_import_offline \
    --where dt=$dt \
    --number-of-shards 9 \
    --repartition false \
    --partition-multiples 9 \
    --index-name ${index} \
    --type-name $type \
    --alias $alias \
    --id $pk \
    --routing $pk \
    --hdfs-work-dir /tmp/es_lucene_file \
    --local-data-dir /mnt/es_lucene_file \
    --bulk-actions=1000 \
    --bulk-size=150 \
    --bulk-flush-interval=15 \
    --index-es-fields=$es \
    --index-hive-fields=$hive
